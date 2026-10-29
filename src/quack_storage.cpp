#include "duckdb/common/exception/binder_exception.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"

#include "quack_storage.hpp"
#include "quack_client.hpp"
#include "quack_secret.hpp"
#include "storage/quack_catalog.hpp"
#include "storage/quack_transaction_manager.hpp"

using namespace duckdb;

//! A path is bare when it carries no host: `ATTACH 'quack:'` (or an empty path, once the "quack:" prefix
//! has been stripped by the ATTACH binder). The endpoint then comes from the secret.
static bool IsBareQuackPath(const string &uri) {
	auto trimmed = uri;
	StringUtil::Trim(trimmed);
	return trimmed == "quack:" || trimmed == "quack://";
}

static unique_ptr<Catalog> QuackAttach(optional_ptr<StorageExtensionInfo> storage_info, ClientContext &context,
                                       AttachedDatabase &db, const string &name, AttachInfo &info,
                                       AttachOptions &attach_options) {
	QuackStorageExtensionInfo::CheckAccess(context, QuackAccess::CONNECT);
	// writes are pushed down to the server as whole statements, so a read-only attach cannot be enforced yet
	if (attach_options.access_mode == AccessMode::READ_ONLY) {
		throw NotImplementedException("READ_ONLY is not currently supported for Quack databases - attach \"%s\" "
		                              "without the READ_ONLY option, or with (READ_WRITE)",
		                              name);
	}
	// the options the core did not consume are ours; the parser lower-cases their names
	static const unordered_set<string> KNOWN_OPTIONS {"token", "secret", "disable_ssl", "client_id",
	                                                  "heartbeat_timeout"};
	for (auto &entry : attach_options.options) {
		if (KNOWN_OPTIONS.find(entry.first) == KNOWN_OPTIONS.end()) {
			throw BinderException("Unrecognized option for attach \"%s\"", entry.first);
		}
	}
	// info.path may or may not already carry the "quack:" prefix.
	auto uri = StringUtil::StartsWith(info.path, "quack:") ? info.path : "quack:" + info.path;

	auto token_entry = attach_options.options.find("token");
	auto secret_entry = attach_options.options.find("secret");
	auto has_token = token_entry != attach_options.options.end();
	auto has_secret_name = secret_entry != attach_options.options.end();
	if (has_token && has_secret_name) {
		throw InvalidInputException("Cannot specify both token and secret - the secret supplies the token");
	}

	// A named secret always decides the token; a bare `quack:` additionally needs a secret to tell us
	// where to connect to. Otherwise the token is resolved further down, when the connection is made.
	auto bare_path = IsBareQuackPath(uri);
	unique_ptr<SecretEntry> secret;
	if (!has_token && (has_secret_name || bare_path)) {
		secret = QuackSecret::Find(
		    context, has_secret_name ? &secret_entry->second : nullptr, bare_path ? "quack:" : uri,
		    bare_path ? QuackSecret::DefaultLookup::ALLOW_SINGLE_SECRET : QuackSecret::DefaultLookup::SCOPE_MATCH_ONLY);
	}
	if (bare_path) {
		// no host given: take the endpoint from the secret scope, falling back to the default host
		string secret_endpoint;
		uri = secret && QuackSecret::TryGetEndpoint(*secret, secret_endpoint) ? secret_endpoint : "quack:localhost";
	}
	auto initial_uri = QuackUri(uri);

	// no ssl on local by default
	auto enable_ssl = !initial_uri.IsLocal();
	auto disable_ssl_entry = attach_options.options.find("disable_ssl");
	if (disable_ssl_entry != attach_options.options.end()) {
		enable_ssl = !disable_ssl_entry->second.GetValue<bool>();
	}
	string token;
	if (has_token) {
		token = token_entry->second.GetValue<string>();
	} else if (secret) {
		token = QuackSecret::GetToken(*secret);
	}
	auto client_id_entry = attach_options.options.find("client_id");
	auto client_id = QuackClient::ResolveClientId(
	    context, client_id_entry != attach_options.options.end() ? &client_id_entry->second : nullptr);
	auto heartbeat_timeout_entry = attach_options.options.find("heartbeat_timeout");
	auto heartbeat_timeout = QuackClient::ResolveHeartbeatTimeout(
	    context, heartbeat_timeout_entry != attach_options.options.end() ? &heartbeat_timeout_entry->second : nullptr);
	QuackUri server_uri(uri, enable_ssl);
	return make_uniq<QuackCatalog>(db, server_uri, context, token, std::move(client_id), heartbeat_timeout);
}

static unique_ptr<TransactionManager> QuackCreateTransactionManager(optional_ptr<StorageExtensionInfo> storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog) {
	auto &quack_catalog = catalog.Cast<QuackCatalog>();
	return make_uniq<QuackTransactionManager>(db, quack_catalog);
}

QuackStorageExtension::QuackStorageExtension() {
	attach = QuackAttach;
	create_transaction_manager = QuackCreateTransactionManager;
	storage_info = make_shared_ptr<QuackStorageExtensionInfo>();
}

QuackStorageExtensionInfo &QuackStorageExtensionInfo::GetState(const DatabaseInstance &instance) {
	auto extension = StorageExtension::Find(instance.config, QuackStorageExtension::STORAGE_EXTENSION_KEY);
	if (!extension) {
		throw InternalException("The quack extension is not loaded");
	}
	return static_cast<QuackStorageExtensionInfo &>(*extension->storage_info);
}

void QuackStorageExtensionInfo::CheckAccess(ClientContext &context, QuackAccess access, const Identifier &catalog) {
	auto &state = GetState(*context.db);
	if (state.access_check) {
		state.access_check(context, access, catalog);
	}
}

void QuackStorageExtensionInfo::RunInBackground(const DatabaseInstance &instance, std::function<void()> task) {
	auto &state = GetState(instance);
	if (!state.scheduler) {
		task();
		return;
	}
	state.scheduler(milliseconds(0), std::move(task));
}

void QuackStorageExtensionInfo::AddServer(QuackServer &server) {
	lock_guard<mutex> guard(servers_lock);
	servers.push_back(server);
}

void QuackStorageExtensionInfo::RemoveServer(QuackServer &server) {
	lock_guard<mutex> guard(servers_lock);
	servers.erase(std::remove_if(servers.begin(), servers.end(),
	                             [&](reference<QuackServer> entry) { return &entry.get() == &server; }),
	              servers.end());
}

vector<QuackConnectionSnapshot> QuackStorageExtensionInfo::GetActiveConnectionSnaps() {
	vector<QuackConnectionSnapshot> result;
	lock_guard<mutex> guard(servers_lock);
	for (auto &server : servers) {
		for (auto &snapshot : server.get().GetActiveConnectionSnap()) {
			result.push_back(std::move(snapshot));
		}
	}
	return result;
}

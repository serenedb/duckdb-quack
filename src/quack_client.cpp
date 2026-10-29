#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/time_point.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include "quack_client.hpp"
#include "quack_secret.hpp"
#include "quack_session_state.hpp"
#include "quack_settings.hpp"
#include "quack_storage.hpp"
#include "quack_uri.hpp"

#include <absl/cleanup/cleanup.h>

namespace duckdb {

milliseconds QuackHeartbeatInterval(idx_t heartbeat_timeout_seconds, RandomEngine &random) {
	auto interval_ms = heartbeat_timeout_seconds * 1000 / 3;
	return milliseconds(static_cast<int64_t>(interval_ms * random.NextRandom(0.8, 1.2)));
}

static optional_idx DeadlineIn(int64_t milliseconds) {
	return optional_idx(NumericCast<idx_t>(TimePoint::Now() + TimePoint::FromNanos(milliseconds * 1000000)));
}

optional_idx QuackControlDeadline() {
	static constexpr int64_t CONTROL_REQUEST_TIMEOUT_MS = 5000;
	return DeadlineIn(CONTROL_REQUEST_TIMEOUT_MS);
}

optional_idx QuackStatementDeadline(ClientContext &context, optional_ptr<const QuackClientConnection> connection) {
	if (context.query_deadline.IsValid() || !connection || !connection->SereneDBPeer()) {
		return context.query_deadline;
	}
	auto timeout_ms =
	    QuackGetUBigintSetting(context, "quack_default_statement_timeout", QUACK_DEFAULT_STATEMENT_TIMEOUT_MS_DEFAULT);
	if (timeout_ms == 0) {
		return optional_idx();
	}
	return DeadlineIn(NumericCast<int64_t>(timeout_ms));
}

template <class T>
string GetUriPart(T ele) {
	if (ele.afterLast - ele.first < 1) {
		throw InvalidInputException("Invalid URI");
	}
	return string(ele.first, ele.afterLast - ele.first);
}

//! Resolve the optional EXTRA_HTTP_HEADERS from the `quack` secret scoped to this URI and add them
//! to `headers`. Silently does nothing when no matching secret / header map is present.
static void LoadExtraHttpHeaders(optional_ptr<ClientContext> context, DatabaseInstance &db, const QuackUri &uri,
                                 HTTPHeaders &headers) {
	auto &secret_manager = SecretManager::Get(db);
	auto transaction = context ? CatalogTransaction::GetSystemCatalogTransaction(*context)
	                           : CatalogTransaction::GetSystemTransaction(db);
	auto match = secret_manager.LookupSecret(transaction, uri.Uri(), "quack");
	if (!match.HasMatch()) {
		return;
	}
	const auto &kv = dynamic_cast<const KeyValueSecret &>(*match.secret_entry->secret);
	Value headers_value;
	if (!kv.TryGetValue("extra_http_headers", headers_value) || headers_value.IsNull()) {
		return;
	}
	for (const auto &entry : MapValue::GetChildren(headers_value)) {
		const auto &kv_pair = StructValue::GetChildren(entry);
		headers.Insert(kv_pair[0].ToString(), kv_pair[1].ToString());
	}
}

void QuackClientConnection::CancelQuery(hugeint_t query_uuid) const {
	auto client = TakeClient(nullptr);
	client->SetOwnerConnection(this);
	client->Request<SuccessResponse>(nullptr, make_uniq<CancelRequestMessage>(connection_id, query_uuid),
	                                 QuackControlDeadline());
	client->SetOwnerConnection(nullptr);
	StoreClient(std::move(client));
}

QuackClient::QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p) : db(db_p), uri(uri_p) {
}

QuackClient::~QuackClient() {
}

optional_idx QuackActiveClientQueryId(ClientContext &context) {
	if (!context.transaction.HasActiveTransaction()) {
		return optional_idx();
	}
	auto raw_query_id = context.transaction.GetActiveQuery();
	if (raw_query_id == DConstants::INVALID_INDEX) {
		return optional_idx();
	}
	return optional_idx(raw_query_id);
}

void QuackClient::EncodeRequest(optional_ptr<ClientContext> context, QuackMessage &message, MemoryStream &out,
                                bool serenedb_peer) {
	if (context) {
		// Inject client_query_id from the active query so client and server logs correlate.
		auto client_query_id = QuackActiveClientQueryId(*context);
		if (client_query_id.IsValid()) {
			message.SetClientQueryId(client_query_id);
		}
	}
	message.ToMemoryStream(out, serenedb_peer);
}

unique_ptr<QuackMessage> QuackClient::DecodeResponse(const string &response_body) {
	MemoryStream read_stream((data_ptr_t)response_body.data(), response_body.size());
	return QuackMessage::FromMemoryStream(read_stream);
}

Logger &QuackClient::GetRequestLogger(optional_ptr<ClientContext> context) {
	return context ? Logger::Get(*context) : Logger::Get(db);
}

void QuackClient::SetRequestLogger(shared_ptr<Logger> logger) {
	request_logger = std::move(logger);
}

void QuackClient::LogRequest(Logger &logger, MessageType request_type, const string &connection_id,
                             optional_idx client_query_id, const string &query, int64_t duration_ms,
                             MessageType response_type, const string &error) {
	if (!logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL)) {
		return;
	}
	// client_id_hash is a server-side identifier (never sent back to the client), so it is empty here.
	auto msg = QuackLogType::ConstructLogMessage(request_type, connection_id, string(), client_query_id, query,
	                                             uri.Http(), duration_ms, response_type, error);
	logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
}

HttpsQuackClient::HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p) : QuackClient(db, uri_p) {};

HttpsQuackClient::~HttpsQuackClient() {
}

string HttpsQuackClient::PostRawLocked(const_data_ptr_t data, idx_t size, optional_idx deadline, MessageType type) {
	D_ASSERT(http_params);
	// a request of a statement ends when the statement's time does, with the statement's timeout error
	ClientContext::DeadlineCheck(deadline);
	http_params->deadline = deadline;
	auto &http_util = HTTPUtil::Get(db);
	auto request_url = uri.Http() + "/quack";
	HTTPHeaders headers = extra_headers;
	if (type == MessageType::CONNECTION_REQUEST) {
		headers.Insert(QUACK_SERENEDB_HEADER, "1");
	}
	PostRequestInfo post_request(request_url, headers, *http_params, data, size);
	unique_ptr<HTTPResponse> response;
	try {
		response = http_util.Request(post_request, http_client);
	} catch (std::exception &ex) {
		ClientContext::DeadlineCheck(deadline);
		ErrorData error(ex);
		throw IOException("Failed to send message: %s", error.Message());
	}
	if (!response || !response->Success()) {
		string error = response ? response->GetError() : "no response";
		throw IOException("Failed to send message: %s", error);
	}
	return std::move(post_request.buffer_out);
}

void HttpsQuackClient::EnsureHttpParams(optional_ptr<ClientContext> context) {
	if (!http_params) {
		auto &http_util = HTTPUtil::Get(db);
		auto request_url = uri.Http() + "/quack";
		if (context && context->transaction.HasActiveTransaction()) {
			http_params = http_util.InitializeParameters(*context, request_url);
		} else {
			http_params = http_util.InitializeParameters(db, request_url);
		}
		// Resolve EXTRA_HTTP_HEADERS from the quack secret once; reused for every request on this client.
		LoadExtraHttpHeaders(context, db, uri, extra_headers);
	}
	http_params->timeout = HTTP_TIMEOUT_SECONDS;
	http_params->retries = 0;
	// http_params is cached across checkouts; re-scope its logger each request so a context-less
	// teardown on a pooled client never logs under a prior query's scope.
	if (request_logger) {
		if (http_params->logger != request_logger) {
			http_params->logger = request_logger;
		}
	} else if (!context) {
		http_params->logger.reset();
	}
}

string HttpsQuackClient::PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size,
                                 optional_idx deadline) {
	lock_guard<mutex> guard(request_mutex);
	EnsureHttpParams(context);
	return PostRawLocked(data, size, deadline, MessageType::INVALID);
}

unique_ptr<QuackMessage> HttpsQuackClient::RequestInternal(optional_ptr<ClientContext> context,
                                                           unique_ptr<QuackMessage> request_message,
                                                           optional_idx deadline) {
	D_ASSERT(request_message);

	lock_guard<mutex> guard(request_mutex);
	EnsureHttpParams(context);

	auto start_time = QuackNowMillis();
	EncodeRequest(context, *request_message, write_stream, owner_connection && owner_connection->SereneDBPeer());
	auto response_body =
	    PostRawLocked(write_stream.GetData(), write_stream.GetPosition(), deadline, request_message->Type());
	auto response_message = DecodeResponse(response_body);
	auto duration_ms = QuackNowMillis() - start_time;

	string error;
	if (response_message->Type() == MessageType::ERROR_RESPONSE) {
		error = response_message->Cast<ErrorResponse>().ErrorMessage();
	}
	LogRequest(GetRequestLogger(context), request_message->Type(), request_message->ConnectionId(),
	           request_message->ClientQueryId(), request_message->LoggableQuery(), duration_ms,
	           response_message->Type(), error);

	return response_message;
}

unique_ptr<QuackClient> QuackClient::GetClient(DatabaseInstance &db, const QuackUri &uri) {
	ExtensionHelper::AutoLoadExtension(db, "httpfs");
	if (!db.ExtensionIsLoaded("httpfs")) {
		throw MissingExtensionException("The quack extension requires the httpfs extension to be loaded!");
	}

	return make_uniq<HttpsQuackClient>(db, uri);
}

unique_ptr<QuackClient> QuackClient::GetClient(ClientContext &context, const QuackUri &uri) {
	return GetClient(*context.db, uri);
}

QuackClientConnection::QuackClientConnection(DatabaseInstance &db_p, unique_ptr<QuackClient> client_p, QuackUri uri_p,
                                             string connection_id_p, idx_t quack_version_p, bool serenedb_peer_p,
                                             idx_t heartbeat_timeout_seconds_p, idx_t max_connections_cached_p)
    : db(db_p), uri(std::move(uri_p)), connection_id(std::move(connection_id_p)), quack_version(quack_version_p),
      serenedb_peer(serenedb_peer_p), heartbeat_timeout_seconds(heartbeat_timeout_seconds_p),
      max_connections_cached(max_connections_cached_p) {
	if (client_p) {
		StoreClient(std::move(client_p));
	}
}

unique_ptr<PrepareRequestMessage> QuackClientConnection::MakePrepare(string sql, hugeint_t query_uuid) const {
	statements_started++;
	return make_uniq<PrepareRequestMessage>(connection_id, std::move(sql), query_uuid);
}

unique_ptr<PrepareResponseMessage> QuackClientConnection::Prepare(ClientContext &context,
                                                                  unique_ptr<PrepareRequestMessage> prepare) const {
	auto query_uuid = prepare->QueryUUID();
	auto client_wrapper = GetClient(context);
	absl::Cleanup cancel_on_failure = [&] {
		CancelQueryInBackground(query_uuid);
	};
	auto response = client_wrapper->GetClient().Request<PrepareResponseMessage>(context, std::move(prepare));
	std::move(cancel_on_failure).Cancel();
	return response;
}

QuackClientConnection::~QuackClientConnection() {
	Close();
}

void QuackClientConnection::Close() {
	if (closed.exchange(true)) {
		return;
	}
	heartbeat.Stop();
	vector<unique_ptr<QuackClient>> clients;
	{
		lock_guard<mutex> guard(lock);
		clients = std::move(cached_clients);
		cached_clients.clear();
	}
	try {
		shared_ptr<QuackClient> client(clients.empty() ? QuackClient::GetClient(db, uri) : std::move(clients.back()));
		// the session ends at the latest once its lease runs out, so nobody waits for the answer
		QuackStorageExtensionInfo::RunInBackground(db, [client, connection_id = connection_id] {
			try {
				client->Request<SuccessResponse>(nullptr, make_uniq<DisconnectMessage>(connection_id),
				                                 QuackControlDeadline());
			} catch (...) { // NOLINT: best effort
			}
		});
	} catch (...) { // NOLINT: best effort, the server ends the session once its lease runs out
	}
}

void QuackClientConnection::CancelQueryInBackground(hugeint_t query_uuid) const {
	try {
		QuackStorageExtensionInfo::RunInBackground(db, [connection = shared_from_this(), query_uuid] {
			try {
				connection->CancelQuery(query_uuid);
			} catch (...) { // NOLINT: best effort, the server may already be gone
			}
		});
	} catch (...) { // NOLINT: best effort, destructors call it
	}
}

void QuackClientConnection::StartHeartbeat() {
	auto random = make_shared_ptr<RandomEngine>();
	heartbeat.Start(
	    QuackStorageExtensionInfo::GetState(db).scheduler,
	    [this, random] { return QuackHeartbeatInterval(heartbeat_timeout_seconds, *random); },
	    [this] { SendHeartbeat(); });
}

//! A failed attempt is not terminal: the worker swallows the throw and retries next interval.
void QuackClientConnection::SendHeartbeat() const {
	if (closed) {
		return;
	}
	auto client = TakeClient(nullptr);
	client->SetOwnerConnection(this);
	client->Request<SuccessResponse>(nullptr, make_uniq<HeartbeatRequestMessage>(connection_id),
	                                 QuackControlDeadline());
	client->SetOwnerConnection(nullptr);
	StoreClient(std::move(client));
}

void QuackClient::ValidateClientId(const string &client_id) {
	// An empty client_id means "no client_id" (opt-out); anything non-empty must carry a little entropy,
	// mirroring the >= 4 minimum QuackServer::ValidateToken enforces on the token.
	if (!client_id.empty() && client_id.size() < 4) {
		throw InvalidInputException("client_id must be at least 4 characters long");
	}
}

string QuackClient::ResolveClientId(ClientContext &context, optional_ptr<const Value> explicit_value) {
	if (explicit_value) {
		if (explicit_value->IsNull()) {
			throw InvalidInputException("client_id cannot be null");
		}
		return explicit_value->GetValue<string>();
	}
	// No explicit value -> fall back to the precomputed default (set from $QUACK_CLIENT_ID or a random
	// per-instance id at load time). An empty setting means the deployment opted out of a default.
	Value setting_val;
	if (context.TryGetCurrentSetting("quack_default_client_id", setting_val) && !setting_val.IsNull()) {
		return setting_val.GetValue<string>();
	}
	return string();
}

void QuackClient::ValidateHeartbeatTimeout(idx_t heartbeat_timeout_seconds) {
	if (heartbeat_timeout_seconds == 0) {
		throw InvalidInputException("heartbeat_timeout must be greater than zero");
	}
	if (heartbeat_timeout_seconds > MAX_HEARTBEAT_TIMEOUT_SECONDS) {
		throw InvalidInputException("heartbeat_timeout is too large");
	}
}

idx_t QuackClient::ResolveHeartbeatTimeout(ClientContext &context, optional_ptr<const Value> explicit_value) {
	Value timeout_value;
	if (explicit_value) {
		if (explicit_value->IsNull()) {
			throw InvalidInputException("heartbeat_timeout cannot be null");
		}
		timeout_value = *explicit_value;
	} else if (!context.TryGetCurrentSetting("quack_default_heartbeat_timeout", timeout_value) ||
	           timeout_value.IsNull()) {
		throw InternalException("quack_default_heartbeat_timeout is not registered");
	}
	auto timeout_seconds = timeout_value.GetValue<idx_t>();
	ValidateHeartbeatTimeout(timeout_seconds);
	return timeout_seconds;
}

string QuackClient::ResolveToken(ClientContext &context, const QuackUri &uri, string token) {
	if (token.empty()) {
		// canonical form, so the scope match does not depend on how the endpoint was spelled
		auto secret = QuackSecret::Find(context, nullptr, uri.CanonicalUri());
		if (secret) {
			token = QuackSecret::GetToken(*secret);
		}
	}
	if (token.empty()) {
		throw InvalidInputException("Could not find a Quack authentication token");
	}
	return token;
}

shared_ptr<QuackClientConnection> QuackClient::ConnectToServer(ClientContext &context, const QuackUri &uri,
                                                               string token, string client_id,
                                                               idx_t heartbeat_timeout_seconds) {
	// Single choke point for every connection path (ATTACH + quack_query), so a malformed client_id is
	// rejected here regardless of where it came from.
	ValidateClientId(client_id);
	ValidateHeartbeatTimeout(heartbeat_timeout_seconds);
	auto server_uri = uri;
	token = ResolveToken(context, uri, std::move(token));

	// open a HTTP client to the server
	auto client = QuackClient::GetClient(context, server_uri);

	// submit the connection request
	auto connection_request_response = client->Request<ConnectionResponseMessage>(
	    context, make_uniq<ConnectionRequestMessage>(token, std::move(client_id), heartbeat_timeout_seconds));
	// Validate the server's selected protocol version before trusting the connection.
	auto quack_version = connection_request_response->QuackVersion();
	if (quack_version < QUACK_MIN_VERSION || quack_version > QUACK_VERSION) {
		throw IOException("Incompatible Quack protocol version: server uses %llu, client supports %llu to %llu",
		                  quack_version, QUACK_MIN_VERSION, QUACK_VERSION);
	}
	auto accepted_heartbeat_timeout_seconds = connection_request_response->HeartbeatTimeoutSeconds();
	ValidateHeartbeatTimeout(accepted_heartbeat_timeout_seconds);
	// success! we got a connection id
	auto connection_id = connection_request_response->ConnectionId();
	// Cache at most one client per async send slot: pending SEND_DATA tasks can check out far more
	// clients than ever POST concurrently, and each cached client pins a server connection slot.
	idx_t pool_size = MaxValue<idx_t>(1, (idx_t)TaskScheduler::GetScheduler(context).NumberOfAsyncThreads());
	return make_shared_ptr<QuackClientConnection>(*context.db, std::move(client), server_uri, std::move(connection_id),
	                                              quack_version, connection_request_response->SereneDB(),
	                                              accepted_heartbeat_timeout_seconds, pool_size);
}

unique_ptr<QuackClient> QuackClientConnection::TakeClient(optional_ptr<ClientContext> context) const {
	unique_ptr<QuackClient> result;
	{
		lock_guard<mutex> guard(lock);
		if (!cached_clients.empty()) {
			result = std::move(cached_clients.back());
			cached_clients.pop_back();
		}
	}
	if (!result) {
		result = QuackClient::GetClient(db, uri);
	}
	result->SetRequestLogger(context ? context->logger : nullptr);
	return result;
}

unique_ptr<QuackClientWrapper> QuackClientConnection::GetClient(ClientContext &context) const {
	if (closed) {
		throw InvalidInputException("The session with the Quack server at %s is closed: its database was detached",
		                            uri.Uri());
	}
	if (server_invalidated) {
		throw InvalidInputException(
		    "The Quack server at %s invalidated its database, so this attached database is gone with it. The "
		    "server has to be restarted; DETACH and ATTACH again to use it afterwards.",
		    uri.Uri());
	}
	// An in-process server shares the attached catalogs with its clients. A statement it runs for connection X
	// must not send a request over X: it would supersede itself and hang.
	if (auto session_state = QuackSessionState::Get(context)) {
		if (session_state->ConnectionId() == connection_id) {
			throw InvalidInputException("A statement cannot route back through its own connection (database %s is "
			                            "attached through the connection this statement runs for)",
			                            uri.Uri());
		}
	}
	auto result = TakeClient(context);
	return make_uniq<QuackClientWrapper>(std::move(result), shared_from_this());
}

void QuackClient::NoteError(const ErrorResponse &error_response) {
	if (!owner_connection) {
		return;
	}
	if (error_response.MustInvalidate()) {
		owner_connection->MarkServerInvalidated();
	}
	if (error_response.Error().RawMessage() == QUACK_INVALID_CONNECTION_ID) {
		owner_connection->MarkLost();
	}
}

void QuackClientConnection::MarkServerInvalidated() const {
	server_invalidated = true;
}

void QuackClientConnection::StoreClient(unique_ptr<QuackClient> client_p) const {
	lock_guard<mutex> guard(lock);
	if (cached_clients.size() >= max_connections_cached) {
		// Beyond the cap, drop the client: destroying it closes its persistent socket, freeing the
		// server-side connection slot instead of retaining an idle keep-alive the server must carry.
		return;
	}
	// A pooled client has no owning query; drop the stamp so later teardown doesn't log under it.
	client_p->SetRequestLogger(nullptr);
	cached_clients.push_back(std::move(client_p));
}

QuackClientWrapper::QuackClientWrapper(unique_ptr<QuackClient> client_p,
                                       shared_ptr<const QuackClientConnection> client_connection_p)
    : client(std::move(client_p)), client_connection(std::move(client_connection_p)) {
	// while checked out, the client knows which attachment it serves, so an error that kills the server can
	// mark that attachment dead (see QuackClient::NoteError)
	client->SetOwnerConnection(client_connection.get());
}

QuackClientWrapper::~QuackClientWrapper() {
	client->SetOwnerConnection(nullptr);
	client_connection->StoreClient(std::move(client));
}

QuackClient &QuackClientWrapper::GetClient() {
	return *client;
}

} // namespace duckdb

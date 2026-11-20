#define DUCKDB_EXTENSION_MAIN

#include <cstdlib>

#include "duckdb/common/types/uuid.hpp"
#include "duckdb/logging/log_manager.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/storage/storage_extension.hpp"
#include "storage/quack_optimizer.hpp"

#include "storage/quack_catalog.hpp"
#include "quack_clear_cache.hpp"
#include "quack_client.hpp"
#include "quack_extension.hpp"
#include "quack_log.hpp"
#include "quack_settings.hpp"
#include "quack_scan.hpp"
#include "quack_scan_from_client.hpp"
#include "quack_secret.hpp"
#include "quack_cancel.hpp"
#include "quack_storage.hpp"
#include "quack_uri.hpp"

namespace duckdb {

static void ValidateHeartbeatTimeoutSetting(ClientContext &, SetScope, Value &parameter) {
	if (parameter.IsNull()) {
		throw InvalidInputException("heartbeat_timeout cannot be null");
	}
	QuackClient::ValidateHeartbeatTimeout(parameter.GetValue<idx_t>());
}

static unique_ptr<BaseSecret> CreateQuackSecretFromConfig(ClientContext &, CreateSecretInput &input) {
	auto scope = input.scope;
	if (scope.empty()) {
		scope.emplace_back("quack:");
	}
	auto secret = make_uniq<KeyValueSecret>(scope, input.type, input.provider, input.name);
	for (const auto &named_param : input.options) {
		auto lower_name = StringUtil::Lower(named_param.first);
		if (lower_name == "token") {
			secret->secret_map["token"] = named_param.second.ToString();
		} else if (lower_name == "extra_http_headers") {
			// Stored as a MAP(VARCHAR, VARCHAR) Value; injected into every quack HTTP request.
			secret->secret_map["extra_http_headers"] = named_param.second;
		} else if (lower_name == "ssl_fingerprint") {
			// normalized at creation, so a malformed fingerprint fails now and not at the first ATTACH
			secret->secret_map["ssl_fingerprint"] = QuackUri::NormalizeFingerprint(named_param.second.ToString());
		} else {
			throw InvalidInputException("Unknown named parameter for quack secret: %s", lower_name);
		}
	}
	secret->redact_keys = {"token"};
	return std::move(secret);
}

static void RegisterQuackSecretType(ExtensionLoader &loader) {
	SecretType secret_type;
	secret_type.name = Identifier(QuackSecret::TYPE);
	secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
	secret_type.default_provider = "config";
	secret_type.extension = "quack";
	loader.RegisterSecretType(secret_type);

	CreateSecretFunction config_fun = {QuackSecret::TYPE, "config", CreateQuackSecretFromConfig};
	config_fun.named_parameters["token"] = LogicalType::VARCHAR;
	config_fun.named_parameters["extra_http_headers"] = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
	config_fun.named_parameters["ssl_fingerprint"] = LogicalType::VARCHAR;
	loader.RegisterFunction(config_fun);
}

static void QuackConnectionIdFunc(const DataChunk &args, ExpressionState &state, Vector &result) {
	auto catalog_name = args.GetValue(0, 0);
	auto &quack_catalog = QuackCatalog::GetQuackCatalog(state.GetContext(), catalog_name);
	result.SetValue(0, Value(quack_catalog.GetClientConnection(state.GetContext())->ConnectionId()));
}

static void LoadInternal(ExtensionLoader &loader) {
	loader.SetDescription("The DuckDB 'Quack' Client/Server Protocol");

	auto &config = DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.AddExtensionOption("quack_prepare_inline_rows",
	                          "Rows returned inline in the PREPARE response before the remainder is left to "
	                          "FETCH; drains whole batches, so it may overshoot to a batch boundary",
	                          LogicalType::UBIGINT, Value::UBIGINT(QUACK_PREPARE_INLINE_ROWS_DEFAULT), nullptr,
	                          SetScope::GLOBAL);

	config.AddExtensionOption("quack_check_peer_data",
	                          "Check every batch a peer sends: its indexes, offsets and lengths, and on a server its "
	                          "values too. Turn it off only when every peer is trusted",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(true), nullptr, SetScope::GLOBAL);

	config.AddExtensionOption("quack_fetch_read_ahead",
	                          "FETCH requests kept in flight ahead of the scan (0 = number of async threads)",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));

	config.AddExtensionOption("quack_target_batch_bytes",
	                          "Target in-memory size of one rebalanced batch (one wire message payload); "
	                          "batches are cut when they reach this size",
	                          LogicalType::UBIGINT, Value::UBIGINT(QUACK_TARGET_BATCH_BYTES_DEFAULT));

	config.AddExtensionOption("quack_rebalance_buffer_bytes",
	                          "Pending (unstamped) bytes the batch rebalancer buffers before gating non-minimum "
	                          "producers (0 = automatic, memory-manager governed)",
	                          LogicalType::UBIGINT, Value::UBIGINT(QUACK_REBALANCE_BUFFER_BYTES_DEFAULT));

	config.AddExtensionOption("quack_server_stream_buffer_bytes",
	                          "Server-side per-stream byte budget, at most a quarter of memory_limit: FETCH counts "
	                          "serialized payload bytes, INSERT counts decoded chunk allocations; 0 disables the limit",
	                          LogicalType::UBIGINT, Value::UBIGINT(QUACK_SERVER_STREAM_BUFFER_BYTES_DEFAULT), nullptr,
	                          SetScope::GLOBAL);

	// Default client_id handed to any ATTACH / quack_query that doesn't pass one explicitly
	string default_client_id;
	if (const char *env_client_id = std::getenv("QUACK_CLIENT_ID")) {
		default_client_id = env_client_id;
	} else {
		default_client_id = UUID::ToString(UUID::GenerateRandomUUID());
	}
	config.AddExtensionOption("quack_default_client_id",
	                          "client_id used when ATTACH / quack_query omit one; precomputed at load from "
	                          "$QUACK_CLIENT_ID (empty opts out) or a random per-instance id. Set to '' to opt out.",
	                          LogicalType::VARCHAR, Value(default_client_id), nullptr, SetScope::GLOBAL);
	config.AddExtensionOption("quack_default_heartbeat_timeout",
	                          "Heartbeat lease timeout in seconds requested by clients when ATTACH / quack_query "
	                          "omit heartbeat_timeout",
	                          LogicalType::UBIGINT, Value::UBIGINT(60), ValidateHeartbeatTimeoutSetting);

	config.AddExtensionOption("quack_default_statement_timeout",
	                          "Milliseconds a statement without statement_timeout may wait for a SereneDB Quack "
	                          "server before it fails, so a server that is gone cannot hold it (0 = no limit)",
	                          LogicalType::UBIGINT, Value::UBIGINT(QUACK_DEFAULT_STATEMENT_TIMEOUT_MS_DEFAULT));

	config.AddExtensionOption("quack_enable_reconnects",
	                          "Enable reconnect support (clients acknowledge results, the server caches the last "
	                          "result until acknowledged)",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false), nullptr, SetScope::GLOBAL);

	config.AddExtensionOption("quack_cache_max_rows",
	                          "Maximum rows the server retains in the result cache (0 = unlimited)",
	                          LogicalType::UBIGINT, Value::UBIGINT(100000), nullptr, SetScope::GLOBAL);

	config.AddExtensionOption("quack_result_ttl",
	                          "Seconds an idle cached result is kept before it is dropped (0 = never)",
	                          LogicalType::UBIGINT, Value::UBIGINT(3600), nullptr, SetScope::GLOBAL);

#ifdef DEBUG
	config.AddExtensionOption("quack_debug_fetch_delay_ms",
	                          "DEBUG SETTING: max random delay in ms before a FETCH response is published, "
	                          "stressing out-of-order batch arrival",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));

	config.AddExtensionOption("quack_debug_emit_delay_ms",
	                          "DEBUG SETTING: max random delay in ms before the fetch collector publishes a "
	                          "batch, stressing out-of-order emission",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));

	config.AddExtensionOption("quack_debug_duplicate_sends",
	                          "DEBUG SETTING: send every SEND_DATA batch two times, imitating a transport retry; "
	                          "the receiver must drop the duplicate",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));

	config.AddExtensionOption("quack_debug_drop_batch",
	                          "DEBUG SETTING: do not send the SEND_DATA batch with this dense index (0 = off); "
	                          "the terminal message must then fail the statement instead of inserting less data",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));

	config.AddExtensionOption("quack_debug_send_delay_ms",
	                          "DEBUG SETTING: max random delay in ms before a SEND_DATA request goes out, "
	                          "stressing out-of-order batch arrival on the server",
	                          LogicalType::UBIGINT, Value::UBIGINT(0));
#endif

	loader.GetDatabaseInstance().GetLogManager().RegisterLogType(make_uniq<QuackLogType>());

	OptimizerExtension quack_optimizer;
	quack_optimizer.optimize_function = QuackOptimizer::Optimize;
	OptimizerExtension::Register(config, std::move(quack_optimizer));

	RegisterQuackSecretType(loader);

	ScalarFunction quack_connection_id("quack_connection_id", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                   QuackConnectionIdFunc);
	quack_connection_id.SetVolatile();
	loader.RegisterFunction(quack_connection_id);

	loader.RegisterFunction(QuackScanFunction::GetFunction());
	loader.RegisterFunction(QuackScanByNameFunction::GetFunction());
	loader.RegisterFunction(QuackScanFromClientFunction::GetFunction());
	loader.RegisterFunction(QuackCancelFunction::GetFunction());
	loader.RegisterFunction(QuackClearCacheFunction::GetFunction());

	loader.RegisterFunction(QuackParseUriFunction::GetFunction());

	StorageExtension::Register(loader.GetDatabaseInstance().config, QuackStorageExtension::STORAGE_EXTENSION_KEY,
	                           duckdb::make_shared_ptr<QuackStorageExtension>());
}

void QuackExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string QuackExtension::Name() {
	return "quack";
}

std::string QuackExtension::Version() const {
#ifdef EXT_VERSION_QUACK
	return EXT_VERSION_QUACK;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(quack, loader) {
	LoadInternal(loader);
}
}

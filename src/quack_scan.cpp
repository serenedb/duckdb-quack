#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "quack_scan.hpp"
#include "quack_client.hpp"
#include "quack_fetch_ahead.hpp"
#include "storage/quack_catalog.hpp"
#include "storage/quack_transaction.hpp"

#include <queue>
namespace duckdb {

//! Run the query of `bind_data` on the server
static unique_ptr<PrepareResponseMessage> RunRemoteQuery(ClientContext &context, QuackScanBindData &bind_data,
                                                         hugeint_t query_uuid) {
	if (bind_data.use_transaction) {
		auto catalog_name = bind_data.catalog_name;
		QuackTransaction::Get(context, QuackCatalog::GetQuackCatalog(context, catalog_name)).ForceStart();
	}
	auto &client_connection = *bind_data.client_connection;
	auto client_wrapper = client_connection.GetClient(context);
	auto prepare = client_connection.MakePrepare(bind_data.query, query_uuid);
	return client_wrapper->GetClient().Request<PrepareResponseMessage>(context, std::move(prepare));
}

//! The query was a statement pushed down by the RemotePushdownOptimizer, and the server ran it (PREPARE runs the
//! query), so the snapshot of the remote catalog is out of date
static void InvalidateCatalog(ClientContext &context, QuackScanBindData &bind_data) {
	auto catalog_name = bind_data.catalog_name;
	QuackCatalog::GetQuackCatalog(context, catalog_name).Invalidate();
}

//! Learn the query's columns from the server, which runs it: the first execution then reads that result.
static void BindRemoteQuery(ClientContext &context, QuackScanBindData &bind_data, vector<LogicalType> &return_types,
                            vector<Identifier> &names) {
	bind_data.query_uuid = UUID::GenerateRandomUUID();
	auto bind_response = RunRemoteQuery(context, bind_data, bind_data.query_uuid);
	bind_data.bind_statement = bind_data.client_connection->StatementsStarted();

	return_types = bind_response->Types();
	bind_data.column_types = return_types;
	for (auto &col_name : bind_response->Names()) {
		names.push_back(Identifier(col_name));
	}
	// the remote query may produce duplicate column names (e.g. SELECT 1 AS x, 2 AS x) - a table
	// function binding requires unique names, so rename the repeats (x, x_1, ...). Columns are
	// referenced positionally everywhere below, so this is purely the name we expose to the binder.
	QueryResult::DeduplicateColumns(names);

	bind_data.results = std::move(bind_response->MutableResults());
	bind_data.needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data.query_uuid = bind_response->QueryUUID();
	if (bind_data.refresh_catalog) {
		InvalidateCatalog(context, bind_data);
	}
}

static unique_ptr<FunctionData> QuackScanBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	// Set logging to be pretty verbose (everything except message payloads)
	if (input.inputs.empty()) {
		throw InternalException("No input to quack scan?");
	}
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("quack_query URI and query parameters cannot be NULL");
	}

	auto query = input.inputs[1].GetValue<string>();
	auto initial_uri = QuackUri(input.inputs[0].GetValue<string>());

	// no ssl on local by default
	auto enable_ssl = !initial_uri.IsLocal();
	auto disable_ssl_entry = input.named_parameters.find("disable_ssl");
	if (disable_ssl_entry != input.named_parameters.end()) {
		enable_ssl = !disable_ssl_entry->second.GetValue<bool>();
	}

	auto bind_data = make_uniq<QuackScanBindData>();
	auto server_uri = QuackUri(initial_uri.Uri(), enable_ssl);

	// An explicit token wins; otherwise ConnectToServer takes it from a quack secret scoped to this URI.
	string token;
	if (input.named_parameters.find("token") != input.named_parameters.end()) {
		token = input.named_parameters["token"].GetValue<string>();
	}
	auto client_id_entry = input.named_parameters.find("client_id");
	auto client_id = QuackClient::ResolveClientId(
	    context, client_id_entry != input.named_parameters.end() ? &client_id_entry->second : nullptr);
	auto heartbeat_timeout_entry = input.named_parameters.find("heartbeat_timeout");
	auto heartbeat_timeout = QuackClient::ResolveHeartbeatTimeout(
	    context, heartbeat_timeout_entry != input.named_parameters.end() ? &heartbeat_timeout_entry->second : nullptr);
	bind_data->client_connection =
	    QuackClient::ConnectToServer(context, server_uri, token, client_id, heartbeat_timeout);
	bind_data->client_connection->StartHeartbeat();
	bind_data->server_uri = server_uri;
	bind_data->token = std::move(token);
	bind_data->client_id = std::move(client_id);
	bind_data->heartbeat_timeout = heartbeat_timeout;
	bind_data->query = std::move(query);
	BindRemoteQuery(context, *bind_data, return_types, names);
	return bind_data;
}

static unique_ptr<FunctionData> QuackScanBindCatalogName(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("catalog_name and query parameters cannot be NULL");
	}
	bool use_transaction = false;
	auto entry = input.named_parameters.find("use_transaction");
	if (entry != input.named_parameters.end()) {
		if (entry->second.IsNull()) {
			throw InvalidInputException("use_transaction cannot be null");
		}
		use_transaction = BooleanValue::Get(entry->second);
	}

	bool refresh_catalog = false;
	auto refresh_entry = input.named_parameters.find("refresh_catalog");
	if (refresh_entry != input.named_parameters.end()) {
		if (refresh_entry->second.IsNull()) {
			throw InvalidInputException("refresh_catalog cannot be null");
		}
		refresh_catalog = BooleanValue::Get(refresh_entry->second);
	}

	auto &catalog = QuackCatalog::GetQuackCatalog(context, input.inputs[0]);
	auto bind_data = make_uniq<QuackScanBindData>();
	bind_data->client_connection = catalog.GetClientConnection(context);
	bind_data->query = input.inputs[1].GetValue<string>();
	bind_data->catalog_name = input.inputs[0];
	bind_data->use_transaction = use_transaction;
	bind_data->refresh_catalog = refresh_catalog;
	BindRemoteQuery(context, *bind_data, return_types, names);
	return bind_data;
}

enum class ChunkResultPushdownType { REQUIRES_PUSHDOWN, PUSHDOWN_ALREADY_APPLIED };

class ChunkResult {
public:
	explicit ChunkResult(DataChunk &chunk_p, ChunkResultPushdownType pushdown_type_p) : pushdown_type(pushdown_type_p) {
		chunk = make_uniq<DataChunk>();
		chunk->InitializeEmpty(chunk_p.GetTypes());
		chunk->Reference(chunk_p);
	}
	DataChunk &Chunk() {
		return *chunk;
	}
	bool RequiresPushdown() const {
		return pushdown_type == ChunkResultPushdownType::REQUIRES_PUSHDOWN;
	}

private:
	unique_ptr<DataChunk> chunk;
	ChunkResultPushdownType pushdown_type;
};

struct QuackScanLocalState : public LocalTableFunctionState {
	explicit QuackScanLocalState() {
	}
	~QuackScanLocalState() override {
	}

	//! Claimed FETCH index of the batch currently being drained; surfaced via get_partition_data
	//! so order-preserving operators (CTAS, COPY TO) can run the scan in parallel without losing order.
	optional_idx current_batch_index;

	//! This thread's outstanding batch claim; persists across BLOCKED yields until the batch arrives.
	optional_idx fetch_claim;
	//! The stream ended below this thread's claim; no more batches for this thread.
	bool fetch_exhausted = false;
	//! This thread already reported its completion to the global scanner countdown.
	bool finish_reported = false;

	queue<ChunkResult> results;
	ColumnDataScanState scan_state;
};

struct QuackScanGlobalState : GlobalTableFunctionState {
	explicit QuackScanGlobalState(vector<ColumnIndex> column_ids_p, vector<idx_t> projection_id_p,
	                              vector<ChunkResult> results_p, bool needs_more_fetch_p, hugeint_t query_uuid_p,
	                              bool reconnects_enabled_p, ChunkResultPushdownType fetch_pushdown_type_p)
	    : max_threads(needs_more_fetch_p ? MAX_THREADS : 1), column_ids(std::move(column_ids_p)),
	      projection_ids(std::move(projection_id_p)), query_uuid(query_uuid_p),
	      reconnects_enabled(reconnects_enabled_p), fetch_pushdown_type(fetch_pushdown_type_p),
	      results(std::move(results_p)) {
	}
	idx_t MaxThreads() const override {
		return max_threads;
	}
	idx_t max_threads;
	vector<ColumnIndex> column_ids;
	vector<idx_t> projection_ids;
	hugeint_t query_uuid;
	//! FETCH read-ahead pipeline; set when the PREPARE response signals more batches to fetch.
	shared_ptr<QuackFetcher> fetcher;
	bool reconnects_enabled;
	//! How every chunk of this scan must be treated, initial batch and fetched continuations
	//! alike. Derived in QuackScanInitGlobal.
	ChunkResultPushdownType fetch_pushdown_type;
	//! Number of active scanners
	atomic<idx_t> active_scanners {0};
	atomic<bool> ack_sent {false};

	vector<ChunkResult> TryGetResults() {
		lock_guard<mutex> guard(lock);
		return std::move(results);
	}

	shared_ptr<QuackClientConnection> connection;

private:
	mutex lock;
	vector<ChunkResult> results;
};

static string BuildPushdownQuery(const QuackScanBindData &bind_data, const TableFunctionInitInput &input) {
	string query;

	// Projection: select only the columns DuckDB actually needs in the output.
	if (!input.column_indexes.empty()) {
		for (auto &col_id : input.column_indexes) {
			if (!query.empty()) {
				query += ", ";
			}
			if (col_id.IsVirtualColumn()) {
				auto virtual_column = col_id.GetPrimaryIndex();
				if (virtual_column == COLUMN_IDENTIFIER_EMPTY) {
					// count(*) marker: the value is never read, so any NULL will do - but it must
					// match the type we declared for it in QuackGetVirtualColumns.
					query += "NULL::BOOLEAN";
				} else if (virtual_column == COLUMN_IDENTIFIER_ROW_ID) {
					throw NotImplementedException("quack does not support rowid on remote tables");
				} else {
					throw InternalException("Unsupported virtual column index");
				}
			} else {
				query += "#" + to_string(col_id.GetPrimaryIndex() + 1);
			}
		}
		query = "SELECT " + query + " ";
	}
	query += StringUtil::Format("FROM %s", bind_data.qualified_table_name.ToString());
	return query;
}

static bool ReconnectsEnabled(ClientContext &context) {
	Value val;
	if (!context.TryGetCurrentSetting("quack_enable_reconnects", val)) {
		return false;
	}
	return val.GetValue<bool>();
}

//! The session a run of the scan uses. A prepared scan runs long after its bind, when the server may have lost the
//! bind's session: the attached database has a new one then, and a scan with its own connection opens it again.
static shared_ptr<QuackClientConnection> ScanConnection(ClientContext &context, QuackScanBindData &bind_data) {
	if (!bind_data.catalog_name.IsNull()) {
		auto catalog_name = bind_data.catalog_name;
		auto &catalog = QuackCatalog::GetQuackCatalog(context, catalog_name);
		// a run that binds nothing joins the statement's transaction here, which forgets a session the server lost
		QuackTransaction::Get(context, catalog);
		return catalog.GetClientConnection(context);
	}
	lock_guard<mutex> guard(bind_data.connection_lock);
	if (bind_data.client_connection->Lost()) {
		auto connection = QuackClient::ConnectToServer(context, bind_data.server_uri, bind_data.token,
		                                               bind_data.client_id, bind_data.heartbeat_timeout);
		connection->StartHeartbeat();
		bind_data.client_connection = std::move(connection);
	}
	return bind_data.client_connection;
}

unique_ptr<GlobalTableFunctionState> QuackScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	auto connection = ScanConnection(context, input.bind_data->CastNoConst<QuackScanBindData>());

	// For the catalog path (ATTACH), LookupEntry only prepares without executing
	// to avoid the server-side result being overwritten by subsequent lookups.
	// We execute the query here, right before scanning, so the result is fresh.
	vector<ChunkResult> results;
	hugeint_t query_uuid;
	// The chunks are already projected only if the query we send carries the projection, which is
	// exactly when BuildPushdownQuery emits a SELECT list - it degrades to a full-width
	// "FROM <table>" on an empty column_indexes.
	auto fetch_pushdown_type = ChunkResultPushdownType::REQUIRES_PUSHDOWN;
	string query;
	bool check_types = false;
	if (!bind_data.qualified_table_name.Path().empty()) {
		query = BuildPushdownQuery(bind_data, input);
		if (!input.column_indexes.empty()) {
			fetch_pushdown_type = ChunkResultPushdownType::PUSHDOWN_ALREADY_APPLIED;
		}
	} else {
		auto &mutable_bind_data = input.bind_data->CastNoConst<QuackScanBindData>();
		// The bind ran the statement for the first execution, so only a read runs again in it: a result the
		// bind left on the server is gone once the connection ran another statement, or once it is a new session
		auto first_execution = !mutable_bind_data.bind_result_taken.exchange(true);
		auto bind_result_dropped =
		    bind_data.needs_more_fetch && bind_data.client_connection->StatementsStarted() != bind_data.bind_statement;
		auto same_session = connection == bind_data.client_connection;
		if (first_execution && same_session && !(bind_data.use_transaction && bind_result_dropped)) {
			for (auto &chunk_ref : bind_data.results) {
				auto &chunk = chunk_ref->Chunk();
				results.emplace_back(chunk, fetch_pushdown_type);
			}
			query_uuid = bind_data.query_uuid;
			auto global_state = make_uniq<QuackScanGlobalState>(
			    input.column_indexes, input.projection_ids, std::move(results), bind_data.needs_more_fetch, query_uuid,
			    ReconnectsEnabled(context), fetch_pushdown_type);
			global_state->connection = connection;
			if (bind_data.needs_more_fetch) {
				global_state->fetcher = make_shared_ptr<QuackFetcher>(context, *connection, query_uuid,
				                                                      QuackFetcher::GetReadAheadDepth(context));
			}
			return std::move(global_state);
		}
		if (bind_data.use_transaction) {
			auto catalog_name = bind_data.catalog_name;
			QuackTransaction::Get(context, QuackCatalog::GetQuackCatalog(context, catalog_name)).ForceStart();
		}
		query = bind_data.query;
		check_types = true;
	}
	query_uuid = UUID::GenerateRandomUUID();
	auto client_wrapper = connection->GetClient(context);
	auto response = client_wrapper->GetClient().Request<PrepareResponseMessage>(
	    context, connection->MakePrepare(std::move(query), query_uuid));
	if (check_types && response->Types() != bind_data.column_types) {
		throw InvalidInputException("The result of the remote query \"%s\" changed its types since it was bound",
		                            bind_data.query);
	}
	auto needs_more_fetch = response->NeedsMoreFetch();
	auto chunks = std::move(response->MutableResults());
	if (check_types && bind_data.refresh_catalog) {
		InvalidateCatalog(context, input.bind_data->CastNoConst<QuackScanBindData>());
	}
	for (auto &chunk_ref : chunks) {
		results.emplace_back(chunk_ref->Chunk(), fetch_pushdown_type);
	}
	// we only multithread if there is more to fetch
	auto global_state =
	    make_uniq<QuackScanGlobalState>(input.column_indexes, input.projection_ids, std::move(results),
	                                    needs_more_fetch, query_uuid, ReconnectsEnabled(context), fetch_pushdown_type);
	global_state->connection = connection;
	if (needs_more_fetch) {
		// start pipelining FETCH requests on the ASYNC pool before the first scan call
		global_state->fetcher =
		    make_shared_ptr<QuackFetcher>(context, *connection, query_uuid, QuackFetcher::GetReadAheadDepth(context));
	}
	return std::move(global_state);
}

unique_ptr<LocalTableFunctionState> QuackScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                       GlobalTableFunctionState *global_state_p) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	auto &global_state = global_state_p->Cast<QuackScanGlobalState>();
	auto local_state = make_uniq<QuackScanLocalState>();

	global_state.active_scanners++;
	auto results = global_state.TryGetResults();
	for (auto &chunk : results) {
		local_state->results.push(std::move(chunk));
	}
	return local_state;
}

static void SendAcknowledgement(ClientContext &context, QuackScanGlobalState &global_state) {
	if (global_state.ack_sent.exchange(true)) {
		return;
	}
	try {
		auto client_wrapper = global_state.connection->GetClient(context);
		client_wrapper->GetClient().Request<SuccessResponse>(
		    context,
		    make_uniq<AcknowledgementMessage>(global_state.connection->ConnectionId(), global_state.query_uuid));
	} catch (const std::exception &e) {
		// The query is already complete, so we swallow the failure rather than fail the query
		DUCKDB_LOG_WARNING(
		    context, StringUtil::Format("Quack: acknowledgement failed and was ignored (query already completed): %s",
		                                e.what()));
	}
}

static void FinishScan(ClientContext &context, QuackScanGlobalState &global_state, QuackScanLocalState &local_state,
                       QuackScanBindData &bind_data) {
	bind_data.completed = true;
	if (local_state.finish_reported) {
		return;
	}
	local_state.finish_reported = true;
	if (global_state.active_scanners.fetch_sub(1) != 1) {
		return;
	}
	if (!global_state.reconnects_enabled) {
		return;
	}
	if (global_state.fetcher) {
		global_state.fetcher->StopAndDrain();
	}
	SendAcknowledgement(context, global_state);
}

static void QuackScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->CastNoConst<QuackScanBindData>();
	auto &global_state = input.global_state->Cast<QuackScanGlobalState>();
	auto &local_state = input.local_state->Cast<QuackScanLocalState>();

	while (true) {
		// first we try to scan from our local results buffer if we have any
		while (!local_state.results.empty()) {
			auto chunk = std::move(local_state.results.front());
			local_state.results.pop();

			auto &response_chunk = chunk.Chunk();
			if (response_chunk.size() > 0) {
				if (!chunk.RequiresPushdown()) {
					output.Reference(response_chunk);
				} else {
					// With filter_prune, projection_ids indexes into column_ids and lists only the
					// columns that reach the output - filter-only columns stay in column_ids but are
					// not emitted. Without it, projection_ids is empty and the output IS column_ids.
					// filter_prune is currently disabled, so the projection_ids branch is not reachable
					// yet; it is written now so that enabling it cannot silently reintroduce the
					// full-width overrun this loop exists to prevent.
					auto &projection_ids = global_state.projection_ids;
					auto output_columns =
					    projection_ids.empty() ? global_state.column_ids.size() : projection_ids.size();
					for (idx_t i = 0; i < output_columns; i++) {
						auto &index = projection_ids.empty() ? global_state.column_ids[i]
						                                     : global_state.column_ids[projection_ids[i]];
						if (index.IsVirtualColumn()) {
							// Handled exactly as BuildPushdownQuery does for the server-side path - keep
							// the two in step: the count(*) marker materializes as NULL, rowid is an error.
							// Note `continue`, not `return`: returning here skipped SetCardinality, and a
							// cardinality of 0 reads to DuckDB as end-of-scan, i.e. a silently EMPTY result.
							auto virtual_column = index.GetPrimaryIndex();
							if (virtual_column == COLUMN_IDENTIFIER_ROW_ID) {
								throw NotImplementedException("quack does not support rowid on remote tables");
							}
							if (virtual_column != COLUMN_IDENTIFIER_EMPTY) {
								throw InternalException("Unsupported virtual column index");
							}
							output.data[i].Reference(Value(output.data[i].GetType()), count_t(response_chunk.size()));
							continue;
						}
						auto col_idx = index.GetPrimaryIndex();
						output.data[i].Reference(response_chunk.data[col_idx]);
					}
					output.CheckCardinality(response_chunk.size());
				}
				return;
			}
		}

		// if that did not work, we consume this thread's claimed batch from the fetcher
		if (local_state.results.empty() && global_state.fetcher && !local_state.fetch_exhausted) {
			idx_t batch_index;
			vector<unique_ptr<DataChunk>> chunks;
			switch (global_state.fetcher->GetBatch(context, input, local_state.fetch_claim, batch_index, chunks)) {
			case QuackFetchResult::BATCH: {
				// tag fetched chunks exactly like the initial batch (see QuackScanInitGlobal)
				for (auto &chunk : chunks) {
					local_state.results.emplace(*chunk, global_state.fetch_pushdown_type);
				}
				local_state.current_batch_index = batch_index;
				continue;
			}
			case QuackFetchResult::FINISHED:
				// server is done, this thread is done
				local_state.fetch_exhausted = true;
				FinishScan(context, global_state, local_state, bind_data);
				return;
			case QuackFetchResult::BLOCKED:
				return;
			}
		}
		// we did not have anything cached and then request to the server did not yield anything - we are done
		FinishScan(context, global_state, local_state, bind_data);
		break;
	}
}

static OperatorPartitionData QuackScanGetPartitionData(ClientContext &, TableFunctionGetPartitionInput &input) {
	auto &local_state = input.local_state->Cast<QuackScanLocalState>();
	// If we haven't received a batch yet, fall back to 0 so downstream doesn't choke; the
	// planner only calls this after QuackScan has returned rows, by which point the current
	// batch index is always set.
	auto idx = local_state.current_batch_index.IsValid() ? local_state.current_batch_index.GetIndex() : 0;
	return OperatorPartitionData(idx);
}

InsertionOrderPreservingMap<string> QuackScanToString(TableFunctionToStringInput &input) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();
	InsertionOrderPreservingMap<string> result;
	result["Server"] = bind_data.client_connection->ServerURI().Uri();
	return result;
}

void QuackScanSerialize(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                        const BoundTableFunction &function) {
	throw NotImplementedException("Quack scans cannot be serialized (yet?)");
}

unique_ptr<FunctionData> QuackScanDeserialize(Deserializer &deserializer, BoundTableFunction &function) {
	throw NotImplementedException("Quack scans cannot be deserialized (yet?)");
}

//! Declare the EMPTY virtual column - the "give me any column, I just need the row count" marker
//! DuckDB uses for count(*); without it count(*) would pick rowid. rowid is declared too, so a
//! query that binds it (one planned locally, not pushed down whole) reaches the scan and gets a
//! clean NotImplemented error rather than an INTERNAL "failed to find referenced virtual column".
static virtual_column_map_t QuackGetVirtualColumns(ClientContext &, optional_ptr<FunctionData>) {
	virtual_column_map_t result;
	result.insert(make_pair(COLUMN_IDENTIFIER_EMPTY, TableColumn("", LogicalType::BOOLEAN)));
	result.insert(make_pair(COLUMN_IDENTIFIER_ROW_ID, TableColumn("rowid", LogicalType::ROW_TYPE)));
	return result;
}

static BindInfo QuackScanGetBindInfo(const optional_ptr<FunctionData> bind_data_p) {
	auto &bind_data = bind_data_p->Cast<QuackScanBindData>();
	BindInfo info(ScanType::EXTERNAL);
	info.table = bind_data.table_entry;
	return info;
}

TableFunction QuackScanFunction::GetFunction() {
	FunctionSignature signature;
	signature.AddParameter("uri", LogicalType::VARCHAR)
	    .AddParameter("query", LogicalType::VARCHAR)
	    .WithTypedKwargs("options", [&](TypedKwargs &options) {
		    options.Add("disable_ssl", LogicalType::BOOLEAN)
		        .Add("token", LogicalType::VARCHAR)
		        .Add("client_id", LogicalType::VARCHAR)
		        .Add("heartbeat_timeout", LogicalType::UBIGINT);
	    });
	auto fun = TableFunction("quack_query", std::move(signature), QuackScan, QuackScanBind, QuackScanInitGlobal,
	                         QuackScanInitLocal);

	fun.projection_pushdown = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.get_virtual_columns = QuackGetVirtualColumns;
	fun.get_bind_info = QuackScanGetBindInfo;
	// fun.filter_pushdown = true;
	// fun.filter_prune = true;
	return fun;
}

TableFunction QuackScanByNameFunction::GetFunction() {
	FunctionSignature signature;
	signature.AddParameter("catalog", LogicalType::VARCHAR)
	    .AddParameter("query", LogicalType::VARCHAR)
	    .WithTypedKwargs("options", [&](TypedKwargs &options) {
		    options.Add("use_transaction", LogicalType::BOOLEAN).Add("refresh_catalog", LogicalType::BOOLEAN);
	    });
	auto fun = TableFunction("quack_query_by_name", std::move(signature), QuackScan, QuackScanBindCatalogName,
	                         QuackScanInitGlobal, QuackScanInitLocal);
	fun.projection_pushdown = true;
	fun.get_partition_data = QuackScanGetPartitionData;
	fun.to_string = QuackScanToString;
	fun.serialize = QuackScanSerialize;
	fun.deserialize = QuackScanDeserialize;
	fun.get_virtual_columns = QuackGetVirtualColumns;
	fun.get_bind_info = QuackScanGetBindInfo;
	// fun.filter_pushdown = true;
	// fun.filter_prune = true;
	return fun;
}

bool QuackCatalog::IsQuackScan(const string &name) {
	return name == "quack_query" || name == "quack_query_by_name";
}

} // namespace duckdb

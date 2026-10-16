#include "duckdb/function/table_function.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/main/secret/secret.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

#include "quack_scan.hpp"
#include "quack_async_request.hpp"
#include "quack_client.hpp"
#include "quack_fetch_ahead.hpp"
#include "quack_storage.hpp"
#include "storage/quack_catalog.hpp"
#include "storage/quack_transaction.hpp"

#include <absl/cleanup/cleanup.h>

#include <queue>
namespace duckdb {

//! Run the query of `bind_data` on the server, or with `describe_only` ask for its columns only
static unique_ptr<PrepareResponseMessage> RunRemoteQuery(ClientContext &context, QuackScanBindData &bind_data,
                                                         hugeint_t query_uuid, bool describe_only = false) {
	if (bind_data.use_transaction) {
		auto catalog_name = bind_data.catalog_name;
		QuackTransaction::Get(context, QuackCatalog::GetQuackCatalog(context, catalog_name)).ForceStart();
	}
	auto &client_connection = *bind_data.client_connection;
	auto client_wrapper = client_connection.GetClient(context);
	auto prepare = client_connection.MakePrepare(bind_data.query, query_uuid);
	prepare->SetDescribeOnly(describe_only);
	// A PREPARE that fails, such as at the statement's deadline, can leave the statement running on the server
	absl::Cleanup cancel_on_failure = [&] {
		try {
			client_connection.CancelQuery(query_uuid);
		} catch (...) { // NOLINT: best effort, the server may already be gone
		}
	};
	auto response = client_wrapper->GetClient().Request<PrepareResponseMessage>(context, std::move(prepare));
	std::move(cancel_on_failure).Cancel();
	return response;
}

//! A statement that changed the remote catalog reloads it, and that ends the statement on the server: read the
//! rest of its result first
static void RefreshAfterStatement(ClientContext &context, QuackScanBindData &bind_data, hugeint_t query_uuid,
                                  vector<unique_ptr<DataChunkWrapper>> &results, bool &needs_more_fetch) {
	auto &client_connection = *bind_data.client_connection;
	if (needs_more_fetch) {
		auto client_wrapper = client_connection.GetClient(context);
		for (idx_t batch_index = 1;; batch_index++) {
			auto fetch_response = client_wrapper->GetClient().Request<FetchResponseMessage>(
			    context, make_uniq<FetchRequestMessage>(client_connection.ConnectionId(), query_uuid, batch_index,
			                                            batch_index - 1));
			if (fetch_response->MutableResults().empty()) {
				break;
			}
			for (auto &chunk : fetch_response->MutableResults()) {
				results.push_back(make_uniq<DataChunkWrapper>(*chunk));
			}
		}
		needs_more_fetch = false;
	}
	auto catalog_name = bind_data.catalog_name;
	QuackCatalog::GetQuackCatalog(context, catalog_name).Refresh(context);
}

//! Learn the query's columns from the server. A server that cannot describe it runs it, and the first execution
//! then reads that result.
static void BindRemoteQuery(ClientContext &context, QuackScanBindData &bind_data, vector<LogicalType> &return_types,
                            vector<Identifier> &names) {
	bind_data.query_uuid = UUID::GenerateRandomUUID();
	auto describe = bind_data.client_connection->QuackVersion() >= QUACK_DESCRIBE_VERSION;
	auto bind_response = RunRemoteQuery(context, bind_data, bind_data.query_uuid, describe);
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

	if (bind_response->Described()) {
		// nothing ran on the server, so every execution runs the query
		bind_data.bind_result_taken = true;
		bind_data.completed = true;
		return;
	}
	bind_data.results = std::move(bind_response->MutableResults());
	bind_data.needs_more_fetch = bind_response->NeedsMoreFetch();
	bind_data.query_uuid = bind_response->QueryUUID();
	if (bind_data.refresh_catalog) {
		// the query was a statement pushed down by the RemotePushdownOptimizer - the server has already
		// executed it (PREPARE runs the query), so reload our snapshot of the remote catalog. Such a
		// statement is always bound on its own, so no catalog entry is in use here
		RefreshAfterStatement(context, bind_data, bind_data.query_uuid, bind_data.results, bind_data.needs_more_fetch);
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
	QuackStorageExtensionInfo::CheckAccess(context, QuackAccess::CONNECT);

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
	    QuackClient::ConnectToServer(context, server_uri, token, std::move(client_id), heartbeat_timeout);
	bind_data->client_connection->StartHeartbeat();
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
	QuackStorageExtensionInfo::CheckAccess(context, QuackAccess::ATTACHED, catalog.GetName());
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
	//! This thread saw the answer of the PREPARE that started the query
	bool started = false;

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

	~QuackScanGlobalState() override {
		if (start && !start_taken) {
			// nobody reads the statement the PREPARE started: end it, so the PREPARE in flight returns
			try {
				connection->CancelQuery(query_uuid);
			} catch (...) { // NOLINT: best effort, the server ends it with the session
			}
		}
		start.reset();
	}

	vector<ChunkResult> TryGetResults() {
		lock_guard<mutex> guard(lock);
		return std::move(results);
	}
	void SetResults(vector<ChunkResult> results_p) {
		lock_guard<mutex> guard(lock);
		results = std::move(results_p);
	}

	shared_ptr<QuackClientConnection> connection;
	//! The PREPARE that starts the query, sent from the async pool: no scan thread waits in it
	unique_ptr<QuackAsyncRequest> start;
	mutex start_lock;
	bool start_taken = false;
	//! The query was bound before, and its result must still have the bound types
	bool check_types = false;

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

unique_ptr<GlobalTableFunctionState> QuackScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<QuackScanBindData>();

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
		// bind left on the server is gone once the connection ran another statement
		auto first_execution = !mutable_bind_data.bind_result_taken.exchange(true);
		auto bind_result_dropped =
		    bind_data.needs_more_fetch && bind_data.client_connection->StatementsStarted() != bind_data.bind_statement;
		if (first_execution && !(bind_data.use_transaction && bind_result_dropped)) {
			for (auto &chunk_ref : bind_data.results) {
				auto &chunk = chunk_ref->Chunk();
				results.emplace_back(chunk, fetch_pushdown_type);
			}
			query_uuid = bind_data.query_uuid;
			auto global_state = make_uniq<QuackScanGlobalState>(
			    input.column_indexes, input.projection_ids, std::move(results), bind_data.needs_more_fetch, query_uuid,
			    ReconnectsEnabled(context), fetch_pushdown_type);
			if (bind_data.needs_more_fetch) {
				global_state->fetcher = make_shared_ptr<QuackFetcher>(context, *bind_data.client_connection, query_uuid,
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
	auto global_state =
	    make_uniq<QuackScanGlobalState>(input.column_indexes, input.projection_ids, vector<ChunkResult>(), true,
	                                    query_uuid, ReconnectsEnabled(context), fetch_pushdown_type);
	global_state->connection = bind_data.client_connection;
	global_state->check_types = check_types;
	auto prepare = bind_data.client_connection->MakePrepare(std::move(query), query_uuid);
	global_state->start = make_uniq<QuackAsyncRequest>(
	    context, *bind_data.client_connection, *prepare,
	    [connection = bind_data.client_connection, query_uuid] { connection->CancelQuery(query_uuid); });
	return std::move(global_state);
}

//! Take the answer of the PREPARE that started the query: its first rows, and the fetcher for the rest. False
//! while the scan task is parked until the answer arrives.
static bool WaitForStart(ClientContext &context, TableFunctionInput &input, QuackScanGlobalState &global_state,
                         QuackScanLocalState &local_state, QuackScanBindData &bind_data) {
	if (local_state.started) {
		return true;
	}
	if (global_state.start) {
		optional_ptr<const InterruptState> interrupt_state;
		if (input.results_execution_mode == AsyncResultsExecutionMode::TASK_EXECUTOR) {
			interrupt_state = input.interrupt_state;
		}
		if (!global_state.start->Ready(interrupt_state)) {
			input.async_result = AsyncResultType::BLOCKED;
			return false;
		}
		lock_guard<mutex> guard(global_state.start_lock);
		if (!global_state.start_taken) {
			global_state.start_taken = true;
			auto message = global_state.start->TakeResponse();
			if (message->Type() != PrepareResponseMessage::TYPE) {
				throw IOException("Expected prepare_response, got %s instead", MessageTypeToString(message->Type()));
			}
			auto &response = message->Cast<PrepareResponseMessage>();
			if (global_state.check_types && response.Types() != bind_data.column_types) {
				throw InvalidInputException(
				    "The result of the remote query \"%s\" changed its types since it was bound", bind_data.query);
			}
			auto needs_more_fetch = response.NeedsMoreFetch();
			auto chunks = std::move(response.MutableResults());
			if (global_state.check_types && bind_data.refresh_catalog) {
				RefreshAfterStatement(context, bind_data, global_state.query_uuid, chunks, needs_more_fetch);
			}
			vector<ChunkResult> results;
			for (auto &chunk_ref : chunks) {
				results.emplace_back(chunk_ref->Chunk(), global_state.fetch_pushdown_type);
			}
			global_state.SetResults(std::move(results));
			if (needs_more_fetch) {
				global_state.fetcher =
				    make_shared_ptr<QuackFetcher>(context, *bind_data.client_connection, global_state.query_uuid,
				                                  QuackFetcher::GetReadAheadDepth(context));
			}
		}
	}
	local_state.started = true;
	for (auto &chunk : global_state.TryGetResults()) {
		local_state.results.push(std::move(chunk));
	}
	return true;
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

static void SendAcknowledgement(ClientContext &context, QuackScanGlobalState &global_state,
                                QuackScanBindData &bind_data) {
	if (global_state.ack_sent.exchange(true)) {
		return;
	}
	try {
		auto client_wrapper = bind_data.client_connection->GetClient(context);
		client_wrapper->GetClient().Request<SuccessResponse>(
		    context,
		    make_uniq<AcknowledgementMessage>(bind_data.client_connection->ConnectionId(), global_state.query_uuid));
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
	SendAcknowledgement(context, global_state, bind_data);
}

static void QuackScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &bind_data = input.bind_data->CastNoConst<QuackScanBindData>();
	auto &global_state = input.global_state->Cast<QuackScanGlobalState>();
	auto &local_state = input.local_state->Cast<QuackScanLocalState>();
	if (!WaitForStart(context, input, global_state, local_state, bind_data)) {
		return;
	}

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

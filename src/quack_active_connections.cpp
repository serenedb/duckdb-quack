#include "quack_active_connections.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "quack_storage.hpp"

namespace duckdb {

static string QueryStateToString(QuackQueryState state) {
	switch (state) {
	case QuackQueryState::IDLE:
		return "idle";
	case QuackQueryState::ACTIVE:
		return "active";
	case QuackQueryState::FINISHED:
		return "finished";
	case QuackQueryState::CANCELLED:
		return "cancelled";
	default:
		return "unknown";
	}
}

//! Per execution, so a prepared statement lists the connections every time it runs
struct QuackActiveConnectionsState : GlobalTableFunctionState {
	vector<QuackConnectionSnapshot> snapshots;
	idx_t offset = 0;
};

static unique_ptr<FunctionData> QuackActiveConnectionsBind(ClientContext &context, TableFunctionBindInput &,
                                                           vector<LogicalType> &return_types,
                                                           vector<Identifier> &names) {
	QuackStorageExtensionInfo::CheckAccess(context, QuackAccess::SERVER);
	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,  LogicalType::VARCHAR,
	                LogicalType::VARCHAR, LogicalType::BIGINT,  LogicalType::TIMESTAMP};
	names = {"server_id", "connection_id", "query", "state", "client_id_hash", "cached_rows", "query_started_at"};
	return make_uniq<TableFunctionData>();
}

static unique_ptr<GlobalTableFunctionState> QuackActiveConnectionsInit(ClientContext &context,
                                                                       TableFunctionInitInput &) {
	auto state = make_uniq<QuackActiveConnectionsState>();
	state->snapshots = QuackStorageExtensionInfo::GetState(*context.db).GetActiveConnectionSnaps();
	return std::move(state);
}

static void QuackActiveConnectionsScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<QuackActiveConnectionsState>();
	idx_t row = 0;
	for (; state.offset < state.snapshots.size() && row < STANDARD_VECTOR_SIZE; state.offset++) {
		auto &snapshot = state.snapshots[state.offset];
		output.data[0].SetValue(row, snapshot.server_id);
		output.data[1].SetValue(row, snapshot.connection_id);
		output.data[2].SetValue(row, snapshot.sql_query);
		output.data[3].SetValue(row, Value(QueryStateToString(snapshot.query_state)));
		output.data[4].SetValue(row, snapshot.client_id_hash.empty() ? Value(LogicalType::VARCHAR)
		                                                             : Value(snapshot.client_id_hash));
		output.data[5].SetValue(row, snapshot.cached_rows.IsValid()
		                                 ? Value::BIGINT(NumericCast<int64_t>(snapshot.cached_rows.GetIndex()))
		                                 : Value(LogicalType::BIGINT));
		output.data[6].SetValue(row, snapshot.query_state == QuackQueryState::IDLE
		                                 ? Value(LogicalType::TIMESTAMP)
		                                 : Value::TIMESTAMP(snapshot.query_started_at));
		row++;
	}
	output.SetChildCardinality(row);
}

TableFunction QuackActiveConnectionsFunction::GetFunction() {
	return TableFunction("quack_active_connections", {}, QuackActiveConnectionsScan, QuackActiveConnectionsBind,
	                     QuackActiveConnectionsInit);
}

} // namespace duckdb

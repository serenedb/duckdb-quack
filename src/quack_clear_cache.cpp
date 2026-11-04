#include "quack_clear_cache.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"

#include "storage/quack_catalog.hpp"

namespace duckdb {

//! Per execution, so a prepared statement clears the caches every time it runs
struct ClearCacheGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

unique_ptr<FunctionData> ClearCacheBind(ClientContext &, TableFunctionBindInput &, vector<LogicalType> &return_types,
                                        vector<Identifier> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return make_uniq<TableFunctionData>();
}

unique_ptr<GlobalTableFunctionState> ClearCacheInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<ClearCacheGlobalState>();
}

void ClearQuackCaches(ClientContext &context) {
	auto databases = DatabaseManager::Get(context).GetDatabases(context);
	for (auto &db_ref : databases) {
		auto &catalog = db_ref->GetCatalog();
		if (catalog.GetCatalogType() != "quack") {
			continue;
		}
		catalog.Cast<QuackCatalog>().Refresh(context);
	}
}

void ClearCacheFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &) {
	auto &state = data_p.global_state->Cast<ClearCacheGlobalState>();
	if (state.finished) {
		return;
	}
	ClearQuackCaches(context);
	state.finished = true;
}

TableFunction QuackClearCacheFunction::GetFunction() {
	return TableFunction("quack_clear_cache", {}, ClearCacheFunction, ClearCacheBind, ClearCacheInitGlobal);
}

} // namespace duckdb

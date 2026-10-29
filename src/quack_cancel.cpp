#include "quack_cancel.hpp"
#include "duckdb.hpp"
#include "quack_client.hpp"
#include "quack_message.hpp"
#include "storage/quack_catalog.hpp"

namespace duckdb {

struct QuackCancelBindData : FunctionData {
	Value catalog_name;
	string target_connection_id;

	QuackCancelBindData(Value catalog_name_p, string connection_id_p)
	    : catalog_name(std::move(catalog_name_p)), target_connection_id(std::move(connection_id_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<QuackCancelBindData>(catalog_name, target_connection_id);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackCancelBindData>();
		return catalog_name == other.catalog_name && target_connection_id == other.target_connection_id;
	}
};

//! Per execution, so a prepared statement cancels every time it runs
struct QuackCancelGlobalState : GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> QuackCancelBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<Identifier> &names) {
	if (input.inputs[0].IsNull() || input.inputs[1].IsNull()) {
		throw BinderException("quack_cancel arguments cannot be NULL");
	}
	// fail at bind on a database that is not a quack one
	QuackCatalog::GetQuackCatalog(context, input.inputs[0]);

	return_types = {LogicalType::VARCHAR, LogicalType::BOOLEAN};
	names = {"connection_id", "cancelled"};
	return make_uniq<QuackCancelBindData>(input.inputs[0], input.inputs[1].GetValue<string>());
}

static unique_ptr<GlobalTableFunctionState> QuackCancelInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<QuackCancelGlobalState>();
}

static void QuackCancelScan(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &data = input.bind_data->Cast<QuackCancelBindData>();
	auto &state = input.global_state->Cast<QuackCancelGlobalState>();
	if (state.finished) {
		return;
	}
	state.finished = true;

	auto catalog_name = data.catalog_name;
	auto &catalog = QuackCatalog::GetQuackCatalog(context, catalog_name);
	auto client_connection = catalog.GetClientConnection();
	auto client_wrapper = client_connection->GetClient(context);
	// Target connection goes in the header; zero UUID = cancel whatever is running
	client_wrapper->GetClient().Request<SuccessResponse>(
	    context, make_uniq<CancelRequestMessage>(data.target_connection_id, hugeint_t {0, 0}));

	output.data[0].SetValue(0, data.target_connection_id);
	output.data[1].SetValue(0, Value::BOOLEAN(true));
	output.SetChildCardinality(1);
}

TableFunction QuackCancelFunction::GetFunction() {
	return TableFunction("quack_cancel",
	                     FunctionSignature()
	                         .AddPositionalOnly("catalog", LogicalType::VARCHAR)
	                         .AddPositionalOnly("connection_id", LogicalType::VARCHAR),
	                     QuackCancelScan, QuackCancelBind, QuackCancelInitGlobal);
}

} // namespace duckdb

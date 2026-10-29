#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "quack_uri.hpp"
#include "quack_client.hpp"

namespace duckdb {

struct QuackScanBindData : FunctionData {
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<QuackScanBindData>();
		return other.client_connection->ConnectionId() == client_connection->ConnectionId() &&
		       other.client_connection->ServerURI() == client_connection->ServerURI() &&
		       other.qualified_table_name == qualified_table_name && other.column_names == column_names &&
		       other.column_types == column_types && other.query == query;
	}
	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<QuackScanBindData>();
		result->client_connection = client_connection;
		result->server_uri = server_uri;
		result->token = token;
		result->client_id = client_id;
		result->heartbeat_timeout = heartbeat_timeout;
		result->qualified_table_name = qualified_table_name;
		result->column_names = column_names;
		result->column_types = column_types;
		result->query = query;
		result->catalog_name = catalog_name;
		result->use_transaction = use_transaction;
		result->refresh_catalog = refresh_catalog;
		// the bind's result belongs to the original, so the copy runs the query again
		result->bind_result_taken = true;
		return std::move(result);
	}

	//! The table to scan, qualified the way the remote server sees it (e.g. s1.child.t). Empty when this
	//! scan does not read a catalog table but the result of a query
	QualifiedName qualified_table_name;
	vector<Identifier> column_names;
	//! The types of the table, or of the query result
	vector<LogicalType> column_types;
	//! The query this scan reads the result of
	string query;
	//! The attached database the scan runs in. Null when the scan has its own connection
	Value catalog_name;
	//! The scan's own connection, as quack_query opened it: a run opens it again once the server lost it
	QuackUri server_uri;
	string token;
	string client_id;
	idx_t heartbeat_timeout = 0;
	mutex connection_lock;
	bool use_transaction = false;
	bool refresh_catalog = false;
	//! A server that cannot only describe the query runs it at bind, and the first execution reads that result.
	//! Any other execution, such as a prepared statement's next one, runs the query.
	idx_t bind_statement = 0;
	atomic<bool> bind_result_taken {false};
	vector<unique_ptr<DataChunkWrapper>> results;
	//! The session the scan was bound on. A run takes the session its database has then (see QuackScanInitGlobal)
	shared_ptr<QuackClientConnection> client_connection;
	optional_ptr<TableCatalogEntry> table_entry;
	bool needs_more_fetch = true;
	hugeint_t query_uuid;
	atomic<bool> completed;

	~QuackScanBindData() override {
		if (!completed && query_uuid != hugeint_t {0, 0}) {
			try {
				client_connection->CancelQuery(query_uuid);
			} catch (...) {
				// server may already be gone
			}
		}
	}
};

class TableFunction;

class QuackScanFunction {
public:
	static TableFunction GetFunction();
};

class QuackScanByNameFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "quack_uri.hpp"
#include "quack_client.hpp"

namespace duckdb {

//! The scan's own session, as quack_query opened it: a run opens it again once the server lost it
class QuackScanSession {
public:
	QuackScanSession(shared_ptr<QuackClientConnection> connection_p, QuackUri server_uri_p, string token_p,
	                 string client_id_p, idx_t heartbeat_timeout_p)
	    : server_uri(std::move(server_uri_p)), token(std::move(token_p)), client_id(std::move(client_id_p)),
	      heartbeat_timeout(heartbeat_timeout_p), connection(std::move(connection_p)) {
	}

	shared_ptr<QuackClientConnection> Get(ClientContext &context) {
		annotated_lock_guard<annotated_mutex> guard(lock);
		if (connection->Lost()) {
			connection = QuackClient::ConnectToServer(context, server_uri, token, client_id, heartbeat_timeout);
		}
		return connection;
	}

private:
	const QuackUri server_uri;
	const string token;
	const string client_id;
	const idx_t heartbeat_timeout;
	annotated_mutex lock;
	shared_ptr<QuackClientConnection> connection DUCKDB_GUARDED_BY(lock);
};

//! The result a server that cannot only describe the query left at bind, by running it. The first execution reads
//! it, any other one, such as a prepared statement's next, runs the query again. Once nothing reads it, an unread
//! rest is cancelled on the server.
struct QuackBindResult {
	QuackBindResult(shared_ptr<QuackClientConnection> connection_p, idx_t statement_p,
	                vector<unique_ptr<DataChunkWrapper>> results_p, bool needs_more_fetch_p, hugeint_t query_uuid_p)
	    : connection(std::move(connection_p)), statement(statement_p), results(std::move(results_p)),
	      needs_more_fetch(needs_more_fetch_p), query_uuid(query_uuid_p) {
	}
	~QuackBindResult() {
		if (!completed) {
			connection->TryCancelQuery(query_uuid);
		}
	}

	//! Whether the execution on `run_connection` reads this result: only the first, on the bind's session, and only
	//! while the result is there. An attached database's session drops it once it ran another statement, while a
	//! quack_query's own session runs no other.
	bool Take(const shared_ptr<QuackClientConnection> &run_connection, bool use_transaction) {
		if (taken.exchange(true) || run_connection != connection) {
			return false;
		}
		auto dropped = needs_more_fetch && connection->StatementsStarted() != statement;
		return !(use_transaction && dropped);
	}

	const shared_ptr<QuackClientConnection> connection;
	//! The statements the session had started when it ran the query
	const idx_t statement;
	vector<unique_ptr<DataChunkWrapper>> results;
	const bool needs_more_fetch;
	const hugeint_t query_uuid;
	atomic<bool> taken {false};
	//! The execution that took the result read it to its end
	atomic<bool> completed {false};
};

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
		result->qualified_table_name = qualified_table_name;
		result->column_names = column_names;
		result->column_types = column_types;
		result->query = query;
		result->catalog_name = catalog_name;
		result->session = session;
		result->use_transaction = use_transaction;
		result->refresh_catalog = refresh_catalog;
		result->client_connection = client_connection;
		result->table_entry = table_entry;
		// the bind's result belongs to the original, so the copy runs the query again
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
	//! The attached database the scan runs in. Null when the scan has its own session
	Value catalog_name;
	//! The scan's own session, null when it runs in an attached database
	shared_ptr<QuackScanSession> session;
	bool use_transaction = false;
	bool refresh_catalog = false;
	//! The session the scan was bound on. A run takes the session its database has then (see QuackScanInitGlobal)
	shared_ptr<QuackClientConnection> client_connection;
	optional_ptr<TableCatalogEntry> table_entry;
	//! Null when the server only described the query
	shared_ptr<QuackBindResult> bind_result;
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

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_statement.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "duckdb/main/statement_iterator.hpp"
#include "duckdb/parser/sql_statement.hpp"

#include "quack_fetch_collector.hpp"

#include <functional>

namespace duckdb {

enum class QuackRunState : uint8_t {
	//! More work is ready: run the next slice
	NOT_READY,
	//! The statement waits for its tasks or for client data: run the next slice once on_reschedule fires
	BLOCKED,
	//! The stream is finished or failed
	FINISHED
};

//! Runs the SQL of one PREPARE on a server-side context and fills the PREPARE's result stream. The host
//! drives it slice by slice, so no thread waits on the statement.
class QuackStatementRunner {
public:
	//! With `describe_only`, a single statement is bound and not run
	QuackStatementRunner(ClientContext &context, shared_ptr<QuackResultStream> stream, string sql, bool describe_only);
	~QuackStatementRunner();

	//! Run one slice. `on_reschedule` fires, from any thread, once a BLOCKED statement can go on.
	QuackRunState Step(const std::function<void()> &on_reschedule);
	//! The server stopped the statement, for a later PREPARE, a cancel or the end of the session. Asked before the
	//! runner finished.
	bool Stopped() const;

private:
	//! Point the session at the stream and parse the SQL. It holds one command, as a prepared statement does in
	//! PostgreSQL.
	void Start();
	//! Bind the statement and close the stream with its columns. False when the command expands to several
	//! statements: binding a later one can need the earlier ones run.
	bool Describe();
	QuackRunState Submit(unique_ptr<SQLStatement> statement);
	//! Run a slice of the submitted statement, and keep its result once it completes
	QuackRunState Execute(const std::function<void()> &on_reschedule);
	//! After the last statement: send its result unless rows went into the stream, and close the stream
	QuackRunState FinishStream();
	//! The statements returned no rows: the last one's Success/Count result becomes batch 1
	void SendLastResult();
	QuackRunState Done();
	//! `error` is a copy: it may come from the result that Fail drops
	QuackRunState Fail(ErrorData error);

	ClientContext &context;
	shared_ptr<QuackResultStream> stream;
	string sql;
	const bool describe_only;
	bool started = false;
	vector<unique_ptr<SQLStatement>> statements;
	idx_t next_statement = 0;
	unique_ptr<QueryResult> running;
	unique_ptr<QueryResult> last_result;
	bool finished = false;
};

} // namespace duckdb

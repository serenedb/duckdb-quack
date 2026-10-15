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

	const shared_ptr<QuackResultStream> &Stream() const {
		return stream;
	}

private:
	//! Submit the next statement, or close the stream after the last one
	QuackRunState SubmitNext();
	//! Bind the only statement and close the stream with its columns. False when the SQL holds several
	//! statements: binding a later one can need the earlier ones run.
	bool Describe();
	//! The statements returned no rows: the last one's Success/Count result becomes batch 1
	void SendLastResult();
	void Fail(ErrorData error);

	ClientContext &context;
	shared_ptr<QuackResultStream> stream;
	string sql;
	bool describe_only;
	unique_ptr<StatementIterator> statements;
	unique_ptr<QueryResult> running;
	unique_ptr<QueryResult> last_result;
	bool finished = false;
};

} // namespace duckdb

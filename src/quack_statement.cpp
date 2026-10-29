#include "quack_statement.hpp"

#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_parameters.hpp"

#include "quack_fetch_collector.hpp"
#include "quack_message.hpp"
#include "quack_session_state.hpp"

namespace duckdb {

QuackStatementRunner::QuackStatementRunner(ClientContext &context_p, shared_ptr<QuackResultStream> stream_p,
                                           string sql_p, bool describe_only_p)
    : context(context_p), stream(std::move(stream_p)), sql(std::move(sql_p)), describe_only(describe_only_p) {
}

QuackStatementRunner::~QuackStatementRunner() {
	if (!finished) {
		Fail(ErrorData(ExceptionType::INTERRUPT, "Interrupted: the statement was abandoned"));
	}
}

QuackRunState QuackStatementRunner::Step(const std::function<void()> &on_reschedule) {
	if (finished) {
		return QuackRunState::FINISHED;
	}
	try {
		if (!running) {
			return SubmitNext();
		}
		if (stream->buffer.HasError()) {
			// an abort that came before the statement started was cleared by its start
			context.Interrupt();
		}
		switch (running->ExecuteTask(on_reschedule)) {
		case QueryResultState::NOT_READY:
			return QuackRunState::NOT_READY;
		case QueryResultState::BLOCKED:
		case QueryResultState::NO_TASKS_AVAILABLE:
			return QuackRunState::BLOCKED;
		case QueryResultState::READY:
		case QueryResultState::FINISHED:
		case QueryResultState::EXECUTION_ERROR:
			break;
		}
		if (running->HasError()) {
			Fail(running->GetErrorObject());
			return QuackRunState::FINISHED;
		}
		// A statement the fetch collector claimed leaves its rows in the stream, any other one keeps them
		auto result = running->TakeCollectorResult();
		if (!result) {
			running->Complete();
			result = std::move(running);
		}
		running.reset();
		if (result->HasError()) {
			Fail(result->GetErrorObject());
			return QuackRunState::FINISHED;
		}
		last_result = std::move(result);
		return QuackRunState::NOT_READY;
	} catch (std::exception &ex) {
		Fail(ErrorData(ex));
		return QuackRunState::FINISHED;
	}
}

QuackRunState QuackStatementRunner::SubmitNext() {
	if (stream->buffer.Finished()) {
		// The server aborted the statement, so no further statement of it runs
		finished = true;
		return QuackRunState::FINISHED;
	}
	// A scan of client data that binds now belongs to this statement. Set here, on the session's thread, and not by
	// the PREPARE: the statement before this one may still be binding.
	if (auto session_state = QuackSessionState::Get(context)) {
		session_state->SetStatement(stream);
	}
	Parse();
	if (describe_only) {
		describe_only = false;
		if (Describe()) {
			finished = true;
			return QuackRunState::FINISHED;
		}
	}
	if (next_statement == statements.size()) {
		if (!stream->Bound()) {
			SendLastResult();
		}
		// Close against the announced total, so a short stream errors instead of truncating.
		stream->buffer.Finish(stream->announced_total);
		finished = true;
		return QuackRunState::FINISHED;
	}
	auto statement = std::move(statements[next_statement++]);
	// The first statement that returns rows sends them into the stream, every other one keeps the default
	// collector (see MakeQuackFetchCollector).
	auto &config = ClientConfig::GetConfig(context);
	config.get_result_collector = [captured = stream](ClientContext &ctx, PreparedStatementData &data) {
		return MakeQuackFetchCollector(ctx, data, captured);
	};
	QueryParameters parameters;
	parameters.result_eagerness = ResultEagerness::FORCED;
	parameters.caller_drives = true;
	try {
		running = context.Submit(std::move(statement), parameters);
	} catch (...) {
		config.get_result_collector = nullptr;
		throw;
	}
	config.get_result_collector = nullptr;
	if (running->HasError()) {
		Fail(running->GetErrorObject());
		return QuackRunState::FINISHED;
	}
	return QuackRunState::NOT_READY;
}

void QuackStatementRunner::Parse() {
	if (parsed) {
		return;
	}
	parsed = true;
	auto iterator = context.IterateStatements(sql);
	idx_t commands = 0;
	while (iterator.Peek()) {
		if (auto statement = iterator.GetStatementForExecution()) {
			statements.push_back(std::move(statement));
		}
		if (iterator.PeelDone() && ++commands > 1) {
			throw ParserException("cannot insert multiple commands into a prepared statement");
		}
	}
}

bool QuackStatementRunner::Describe() {
	if (statements.size() != 1) {
		return false;
	}
	auto signature = context.BindStatement(std::move(statements[0]));
	next_statement = 1;
	vector<string> names;
	names.reserve(signature.names.size());
	for (auto &name : signature.names) {
		names.push_back(name.GetIdentifierName());
	}
	stream->SignalBound(std::move(signature.types), std::move(names), true);
	stream->announced_total = 0;
	stream->buffer.Finish(stream->announced_total);
	return true;
}

void QuackStatementRunner::SendLastResult() {
	if (!last_result || last_result->GetNames().empty()) {
		stream->buffer.SetError(ErrorData(ExceptionType::INVALID_INPUT, "Query did not return any columns"));
		return;
	}
	// BaseQueryResult::names is a vector<Identifier>. The stream carries plain strings.
	vector<string> result_names;
	result_names.reserve(last_result->GetNames().size());
	for (auto &col_name : last_result->GetNames()) {
		result_names.push_back(col_name.GetIdentifierName());
	}
	stream->SignalBound(last_result->GetTypes(), std::move(result_names), false);
	unique_ptr<QuackChunkPayloadWriter> writer;
	idx_t rows = 0;
	while (auto chunk = last_result->Fetch()) {
		if (chunk->size() == 0) {
			continue;
		}
		if (!writer) {
			writer = make_uniq<QuackChunkPayloadWriter>(0, stream->serenedb_peer);
		}
		writer->AppendChunk(*chunk);
		rows += chunk->size();
	}
	if (!writer) {
		stream->announced_total = 0;
		return;
	}
	auto sealed = writer->Seal();
	QuackFetchPayload entry;
	entry.payload = std::move(sealed.payload);
	entry.payload_size = sealed.payload_size;
	entry.chunk_count = sealed.chunk_count;
	entry.rows = rows;
	auto bytes = entry.payload_size;
	stream->buffer.PushBatch(1, std::move(entry), bytes);
	stream->announced_total = 1;
}

void QuackStatementRunner::Fail(ErrorData error) {
	finished = true;
	running.reset();
	stream->buffer.SetError(error);
	// A failed statement must stop the client's sends, so its scan's streams get the error too.
	if (auto session_state = QuackSessionState::Get(context)) {
		session_state->Streams().Fail(error);
	}
}

} // namespace duckdb

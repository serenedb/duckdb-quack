#include "quack_statement.hpp"

#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_parameters.hpp"

#include "quack_fetch_collector.hpp"
#include "quack_message.hpp"
#include "quack_session_state.hpp"

#include <absl/cleanup/cleanup.h>

namespace duckdb {

QuackStatementRunner::QuackStatementRunner(ClientContext &context_p, shared_ptr<QuackResultStream> stream_p,
                                           string sql_p, bool describe_only_p)
    : context(context_p), stream(std::move(stream_p)), sql(std::move(sql_p)), describe_only(describe_only_p) {
}

QuackStatementRunner::~QuackStatementRunner() {
	if (finished) {
		return;
	}
	ErrorData error(ExceptionType::INTERRUPT, "Interrupted: the statement was abandoned");
	if (started) {
		Fail(std::move(error));
	} else {
		stream->buffer.SetError(std::move(error));
	}
}

bool QuackStatementRunner::Stopped() const {
	return stream->buffer.Finished();
}

QuackRunState QuackStatementRunner::Step(const std::function<void()> &on_reschedule) {
	if (finished) {
		return QuackRunState::FINISHED;
	}
	// Any error ends the statement and goes to the client in its stream
	try {
		if (running) {
			return Execute(on_reschedule);
		}
		if (Stopped()) {
			// no further statement of it runs
			return Done();
		}
		if (!started) {
			Start();
			if (describe_only && Describe()) {
				return Done();
			}
		}
		if (next_statement == statements.size()) {
			return FinishStream();
		}
		return Submit(std::move(statements[next_statement++]));
	} catch (std::exception &ex) {
		return Fail(ErrorData(ex));
	}
}

void QuackStatementRunner::Start() {
	started = true;
	// A scan of client data that binds from now on belongs to this statement. Set here, on the session's thread, and
	// not by the PREPARE: the statement before this one may still be binding.
	if (auto session_state = QuackSessionState::Get(context)) {
		session_state->SetStatement(stream);
	}
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
	stream->SignalBound(std::move(signature.types), signature.names, true);
	stream->announced_total = 0;
	stream->buffer.Finish(stream->announced_total);
	return true;
}

QuackRunState QuackStatementRunner::Submit(unique_ptr<SQLStatement> statement) {
	// The first statement that returns rows sends them into the stream, every other one keeps the default
	// collector (see MakeQuackFetchCollector).
	auto &config = ClientConfig::GetConfig(context);
	config.get_result_collector = [captured = stream](ClientContext &ctx, PreparedStatementData &data) {
		return MakeQuackFetchCollector(ctx, data, captured);
	};
	absl::Cleanup reset_collector = [&config] {
		config.get_result_collector = nullptr;
	};
	QueryParameters parameters;
	parameters.result_eagerness = ResultEagerness::FORCED;
	parameters.caller_drives = true;
	running = context.Submit(std::move(statement), parameters);
	std::move(reset_collector).Invoke();
	if (running->HasError()) {
		return Fail(running->GetErrorObject());
	}
	return QuackRunState::NOT_READY;
}

QuackRunState QuackStatementRunner::Execute(const std::function<void()> &on_reschedule) {
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
		return Fail(running->GetErrorObject());
	}
	// A statement the fetch collector claimed leaves its rows in the stream, any other one keeps them
	auto result = running->TakeCollectorResult();
	if (!result) {
		running->Complete();
		result = std::move(running);
	}
	running.reset();
	if (result->HasError()) {
		return Fail(result->GetErrorObject());
	}
	last_result = std::move(result);
	return QuackRunState::NOT_READY;
}

QuackRunState QuackStatementRunner::FinishStream() {
	if (!stream->Bound()) {
		SendLastResult();
	}
	// Close against the announced total, so a short stream errors instead of truncating.
	stream->buffer.Finish(stream->announced_total);
	return Done();
}

void QuackStatementRunner::SendLastResult() {
	if (!last_result || last_result->GetNames().empty()) {
		stream->buffer.SetError(ErrorData(ExceptionType::INVALID_INPUT, "Query did not return any columns"));
		return;
	}
	stream->SignalBound(last_result->GetTypes(), last_result->GetNames(), false);
	unique_ptr<QuackChunkPayloadWriter> writer;
	idx_t rows = 0;
	while (auto chunk = last_result->Fetch()) {
		if (chunk->size() == 0) {
			continue;
		}
		if (!writer) {
			writer = make_uniq<QuackChunkPayloadWriter>(0, stream->wire_version);
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

QuackRunState QuackStatementRunner::Done() {
	finished = true;
	return QuackRunState::FINISHED;
}

QuackRunState QuackStatementRunner::Fail(ErrorData error) {
	running.reset();
	stream->buffer.SetError(error);
	// A failed statement must stop the client's sends, so its scan's streams get the error too.
	if (auto session_state = QuackSessionState::Get(context)) {
		session_state->Streams().Fail(error);
	}
	return Done();
}

} // namespace duckdb

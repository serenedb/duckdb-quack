#include "quack_async_request.hpp"

#include "duckdb/logging/logger.hpp"

#include "quack_log.hpp"

namespace duckdb {

class QuackAsyncRequest::Task : public AsyncTask {
public:
	Task(shared_ptr<State> state_p, unique_ptr<QuackClientWrapper> client_p, unique_ptr<MemoryStream> payload_p,
	     idx_t payload_size_p, optional_idx deadline_p, shared_ptr<Logger> logger_p, MessageType type_p,
	     string connection_id_p, optional_idx client_query_id_p, string query_p, std::function<void()> on_failure_p)
	    : state(std::move(state_p)), client(std::move(client_p)), payload(std::move(payload_p)),
	      payload_size(payload_size_p), deadline(deadline_p), logger(std::move(logger_p)), type(type_p),
	      connection_id(std::move(connection_id_p)), client_query_id(client_query_id_p), query(std::move(query_p)),
	      on_failure(std::move(on_failure_p)) {
	}

	void Execute() override {
		unique_ptr<QuackMessage> response;
		ErrorData error;
		try {
			auto &quack_client = client->GetClient();
			auto start_time = QuackNowMillis();
			auto body = quack_client.PostRaw(nullptr, payload->GetData(), payload_size, deadline);
			response = QuackClient::DecodeResponse(body);
			string error_text;
			if (response->Type() == MessageType::ERROR_RESPONSE) {
				auto &error_response = response->Cast<ErrorResponse>();
				quack_client.NoteError(error_response);
				error_text = error_response.ErrorMessage();
				error = error_response.Error();
			}
			quack_client.LogRequest(Logger::Get(logger), type, connection_id, client_query_id, query,
			                        QuackNowMillis() - start_time, response->Type(), error_text);
		} catch (std::exception &ex) {
			error = ErrorData(ex);
		}
		// back in the connection's pool before anyone wakes
		client.reset();
		if (error.HasError()) {
			response.reset();
			if (on_failure) {
				try {
					on_failure();
				} catch (...) { // NOLINT: best effort, the request's error is the one to report
				}
			}
		}
		{
			annotated_lock_guard<annotated_mutex> guard(state->lock);
			state->done = true;
			state->response = std::move(response);
			state->error = std::move(error);
		}
		state->completion->FinishIOTask();
	}

private:
	shared_ptr<State> state;
	unique_ptr<QuackClientWrapper> client;
	unique_ptr<MemoryStream> payload;
	idx_t payload_size;
	optional_idx deadline;
	shared_ptr<Logger> logger;
	MessageType type;
	string connection_id;
	optional_idx client_query_id;
	string query;
	std::function<void()> on_failure;
};

QuackAsyncRequest::QuackAsyncRequest(ClientContext &context, const QuackClientConnection &connection,
                                     QuackMessage &request, std::function<void()> on_failure)
    : state(make_shared_ptr<State>()), queue(make_uniq<ManagedAsyncTaskQueue>(context, 1)) {
	auto payload = make_uniq<MemoryStream>();
	QuackClient::EncodeRequest(context, request, *payload);
	auto payload_size = payload->GetPosition();
	state->completion->AddIOTask();
	queue->Register(make_uniq<Task>(state, connection.GetClient(context), std::move(payload), payload_size,
	                                context.query_deadline, context.logger, request.Type(), connection.ConnectionId(),
	                                QuackActiveClientQueryId(context), request.LoggableQuery(), std::move(on_failure)),
	                payload_size);
}

QuackAsyncRequest::~QuackAsyncRequest() {
	try {
		queue->Close();
	} catch (...) { // NOLINT: the task reports its error through the state
	}
}

bool QuackAsyncRequest::Done() {
	annotated_lock_guard<annotated_mutex> guard(state->lock);
	return state->done;
}

bool QuackAsyncRequest::Ready(optional_ptr<const InterruptState> interrupt_state) {
	if (Done()) {
		return true;
	}
	if (interrupt_state && state->completion->TryPark(*interrupt_state)) {
		return false;
	}
	annotated_lock_guard<annotated_mutex> guard(state->lock);
	auto done = [this]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
		return state->done;
	};
	state->lock.Await(absl::Condition(&done));
	return true;
}

unique_ptr<QuackMessage> QuackAsyncRequest::TakeResponse() {
	annotated_lock_guard<annotated_mutex> guard(state->lock);
	D_ASSERT(state->done);
	if (state->error.HasError()) {
		state->error.Throw();
	}
	return std::move(state->response);
}

} // namespace duckdb

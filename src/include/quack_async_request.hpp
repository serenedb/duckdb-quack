//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_async_request.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/serializer/async_task_queue.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/scan_read_ahead.hpp"

#include "quack_client.hpp"
#include "quack_message.hpp"

#include <functional>

namespace duckdb {

//! One request sent from an async-pool thread, so the task that needs its answer gives its thread back while the
//! server works instead of waiting in it.
class QuackAsyncRequest {
public:
	//! Encodes `request` now and sends it at once. `on_failure` runs when the request fails.
	QuackAsyncRequest(ClientContext &context, const QuackClientConnection &connection, QuackMessage &request,
	                  std::function<void()> on_failure);
	~QuackAsyncRequest();

	//! True once the answer arrived. Otherwise parks the task of `interrupt_state` until it does and returns
	//! false, or, without an interrupt state, waits for it.
	bool Ready(optional_ptr<const InterruptState> interrupt_state);
	//! The answer: the response message, or the request's error thrown
	unique_ptr<QuackMessage> TakeResponse();

private:
	struct State {
		annotated_mutex lock;
		bool done DUCKDB_GUARDED_BY(lock) = false;
		unique_ptr<QuackMessage> response DUCKDB_GUARDED_BY(lock);
		ErrorData error DUCKDB_GUARDED_BY(lock);
		shared_ptr<ReadAheadJobCompletion> completion = make_shared_ptr<ReadAheadJobCompletion>(nullptr);
	};

	bool Done();
	class Task;

	shared_ptr<State> state;
	unique_ptr<ManagedAsyncTaskQueue> queue;
};

} // namespace duckdb

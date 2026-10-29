#pragma once

#include <functional>
#include <thread>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

//! A background thread that runs `tick` roughly every `interval()` until stopped.
class QuackPeriodicWorker {
public:
	~QuackPeriodicWorker() {
		Stop();
	}

	void Start(std::function<milliseconds()> interval_p, std::function<void()> tick_p) {
		D_ASSERT(!thread.joinable());
		interval = std::move(interval_p);
		tick = std::move(tick_p);
#if defined(__EMSCRIPTEN__) && !defined(__EMSCRIPTEN_PTHREADS__)
		// Single-threaded wasm cannot start a thread, so the tick never runs there
		// TODO: drive the tick from the event loop instead (emscripten_set_interval)
#else
		thread = std::thread(&QuackPeriodicWorker::Loop, this);
#endif
	}

	//! Wake the worker and join its thread
	void Stop() {
		{
			lock_guard<mutex> guard(lock);
			stopping = true;
		}
		if (thread.joinable()) {
			thread.join();
		}
	}

private:
	void Loop() {
		auto stopped = [this]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return stopping;
		};
		while (true) {
			{
				lock_guard<mutex> guard(lock);
				if (lock.AwaitWithTimeout(absl::Condition(&stopped), absl::FromChrono(interval()))) {
					return;
				}
			}
			try {
				tick();
			} catch (...) { // NOLINT: a failed tick is retried on the next one
			}
		}
	}

	std::function<milliseconds()> interval;
	std::function<void()> tick;
	std::thread thread;
	mutex lock;
	bool stopping = false;
};

} // namespace duckdb

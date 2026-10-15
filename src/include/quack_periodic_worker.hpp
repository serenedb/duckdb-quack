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
		thread = std::thread(&QuackPeriodicWorker::Loop, this);
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
					break;
				}
			}
			try {
				tick();
			} catch (...) {
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

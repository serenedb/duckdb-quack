#pragma once

#include <functional>
#include <thread>

#include "duckdb/common/assert.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {

//! Runs a task after a delay, on the host's background threads
using QuackScheduler = std::function<void(milliseconds delay, std::function<void()> task)>;

//! Runs `tick` roughly every `interval()` until stopped: on the host's scheduler when there is one, otherwise on a
//! thread of its own.
class QuackPeriodicWorker {
public:
	~QuackPeriodicWorker() {
		Stop();
	}

	void Start(const QuackScheduler &scheduler, std::function<milliseconds()> interval, std::function<void()> tick) {
		D_ASSERT(!state);
		state = make_shared_ptr<State>();
		state->interval = std::move(interval);
		state->tick = std::move(tick);
		if (scheduler) {
			state->scheduler = scheduler;
			Arm(state);
		} else {
#if defined(__EMSCRIPTEN__) && !defined(__EMSCRIPTEN_PTHREADS__)
			// Single-threaded wasm cannot start a thread, so the tick never runs there
			// TODO: drive the tick from the event loop instead (emscripten_set_interval)
#else
			thread = std::thread(&QuackPeriodicWorker::Loop, state);
#endif
		}
	}

	//! No tick starts after this returns, and a running one has finished
	void Stop() {
		if (!state) {
			return;
		}
		{
			lock_guard<mutex> guard(state->lock);
			state->stopping = true;
			auto idle = [this]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
				return !state->running;
			};
			state->lock.Await(absl::Condition(&idle));
		}
		if (thread.joinable()) {
			thread.join();
		}
	}

private:
	struct State {
		mutex lock;
		bool stopping = false;
		bool running = false;
		std::function<milliseconds()> interval;
		std::function<void()> tick;
		QuackScheduler scheduler;
	};

	//! Run the tick unless the worker stopped. False once it has.
	static bool Tick(State &state) {
		{
			lock_guard<mutex> guard(state.lock);
			if (state.stopping) {
				return false;
			}
			state.running = true;
		}
		try {
			state.tick();
		} catch (...) { // NOLINT: a failed tick is retried on the next one
		}
		lock_guard<mutex> guard(state.lock);
		state.running = false;
		return !state.stopping;
	}

	static void Arm(shared_ptr<State> state) {
		auto delay = state->interval();
		state->scheduler(delay, [state] {
			if (Tick(*state)) {
				Arm(state);
			}
		});
	}

	static void Loop(shared_ptr<State> state) {
		auto stopped = [&state]() DUCKDB_NO_THREAD_SAFETY_ANALYSIS {
			return state->stopping;
		};
		while (true) {
			{
				lock_guard<mutex> guard(state->lock);
				if (state->lock.AwaitWithTimeout(absl::Condition(&stopped), absl::FromChrono(state->interval()))) {
					return;
				}
			}
			if (!Tick(*state)) {
				return;
			}
		}
	}

	shared_ptr<State> state;
	std::thread thread;
};

} // namespace duckdb

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_extension.hpp"

#include "quack_periodic_worker.hpp"
#include "quack_server.hpp"

#include <functional>

namespace duckdb {

class QuackStorageExtension : public StorageExtension {
public:
	QuackStorageExtension();

	static constexpr const char *STORAGE_EXTENSION_KEY = "quack";
};

//! The quack servers running on a database: a server registers itself for its lifetime
class QuackStorageExtensionInfo : public StorageExtensionInfo {
public:
	static QuackStorageExtensionInfo &GetState(const DatabaseInstance &instance);
	//! Run `task`, which waits on a peer, on `scheduler` so no thread waits for it; without a scheduler, run it now
	static void RunInBackground(const DatabaseInstance &instance, std::function<void()> task);

	void AddServer(QuackServer &server);
	void RemoveServer(QuackServer &server);
	//! The connections of every server, as quack_active_connections() lists them
	vector<QuackConnectionSnapshot> GetActiveConnectionSnaps();

	//! Where heartbeats and the requests nobody waits for run; without one, each heartbeat has a thread of its own
	QuackScheduler scheduler;

private:
	mutex servers_lock;
	vector<reference<QuackServer>> servers;
};

} // namespace duckdb

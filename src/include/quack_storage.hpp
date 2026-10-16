#pragma once

#include "duckdb/common/identifier.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"
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

//! What a quack function does for its session, for the host to allow or refuse
enum class QuackAccess : uint8_t {
	//! open a session on a server: ATTACH, quack_query with an address
	CONNECT,
	//! use the sessions of an attached database: quack_query_by_name, quack_cancel, quack_connection_id
	ATTACHED,
	//! read or change this server's sessions or identity: quack_active_connections, quack_identify
	SERVER,
};

//! Throws to refuse; `catalog` names the attached database of ATTACHED
using QuackAccessCheck = std::function<void(ClientContext &context, QuackAccess access, const Identifier &catalog)>;

//! The quack servers running on a database: a server registers itself for its lifetime
class QuackStorageExtensionInfo : public StorageExtensionInfo {
public:
	static QuackStorageExtensionInfo &GetState(const DatabaseInstance &instance);
	static void CheckAccess(ClientContext &context, QuackAccess access, const Identifier &catalog = Identifier());

	void AddServer(QuackServer &server);
	void RemoveServer(QuackServer &server);
	//! The connections of every server, as quack_active_connections() lists them
	vector<QuackConnectionSnapshot> GetActiveConnectionSnaps();

	//! Set before the database runs a query
	QuackAccessCheck access_check;
	//! Where heartbeats run; without one, each heartbeat has a thread of its own
	QuackScheduler scheduler;
	//! Whether `host`:`port` reaches a quack listener of this database. A session on itself would wait for its own
	//! threads, so the client refuses one.
	std::function<bool(const string &host, uint16_t port)> serves;

private:
	mutex servers_lock;
	vector<reference<QuackServer>> servers;
};

} // namespace duckdb

#pragma once

#include <queue>
#include <thread>

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"

#include "quack_result_cache.hpp"
#include "quack_uri.hpp"

namespace duckdb {

class ClientContext;
class QuackMessage;
class Connection;
class MemoryStream;
class DatabaseInstance;
struct QuackResultStream;

enum class QuackQueryState : uint8_t { IDLE, ACTIVE, FINISHED, CANCELLED };

//! The stream the connection's active query fills, one at a time. The query runs on a background
//! thread with a rebalancing result collector, and the FETCH handlers drain the stream's buffer.
struct QuackStatementState {
	annotated_mutex lock;
	shared_ptr<QuackResultStream> stream DUCKDB_GUARDED_BY(lock);
	std::thread thread DUCKDB_GUARDED_BY(lock);
	hugeint_t query_uuid DUCKDB_GUARDED_BY(lock) = 0;
	//! A late FETCH for this query_uuid gets this error. The stream and its payloads can then go away.
	ErrorData abort_error DUCKDB_GUARDED_BY(lock);
};

struct QuackConnection {
	QuackConnection(string connection_id_p, idx_t heartbeat_timeout_seconds_p);
	~QuackConnection();

	//! Renew unless the timeout has already elapsed. Once expired, a lease cannot be revived.
	bool TryRenewLease();
	//! True if the lease timeout has elapsed, latching the expiry ("cannot be revived").
	bool LeaseExpiredLocked(time_point<steady_clock> now) DUCKDB_REQUIRES(lease_lock);

	//! Guards the session state below and the result cache. Never held across a statement.
	mutex lock;
	//! Held for a whole statement, because `duckdb_connection` runs one at a time. Only the query
	//! driver takes it. Request handlers must not.
	mutex execution_lock;
	unique_ptr<Connection> duckdb_connection;
	//! Replay cache of the last client query's result stream, null unless quack_enable_reconnects.
	unique_ptr<QuackResultCache> result_cache;
	//! The owning server's live-cache counter, handed to every cache this connection creates.
	shared_ptr<atomic<idx_t>> live_caches;
	//! Rows held by result_cache
	atomic<idx_t> cached_rows {DConstants::INVALID_INDEX};
	string connection_id;

	void SyncCachedRows() {
		cached_rows = result_cache ? result_cache->retained_rows : DConstants::INVALID_INDEX;
	}

	//! The only way to drop the cache, keeps the lock-free cached_rows mirror in sync with the drop
	void ClearResultCache() {
		result_cache.reset();
		SyncCachedRows();
	}

	//! True while this connection holds its slot in the server's cache-expiry queue (guarded by `lock`).
	//! The slot outlives any one cache: the sweep re-queues it while a cache exists and releases it otherwise.
	bool cache_in_expiry_queue = false;

	//! Stable per-client reconnect key: HMAC-SHA256(server_hmac_key, client_id). Intentionally excludes
	//! connection_id so it stays identical across (re)connections for the same client_id. Empty if no client_id.
	string client_id_hash;

	//! Heartbeat and lease variables
	const idx_t heartbeat_timeout_seconds;
	annotated_mutex lease_lock;
	time_point<steady_clock> lease_last_renewed_at DUCKDB_GUARDED_BY(lease_lock);
	bool lease_expired DUCKDB_GUARDED_BY(lease_lock) = false;

	string sql_query;
	//! Read unlocked by the cache sweep and the connection listing, so it must be atomic.
	atomic<QuackQueryState> query_state {QuackQueryState::IDLE};
	timestamp_t query_started_at {0};

	//! Only an ACTIVE query can finish, so a concurrent CANCEL is not overwritten
	void MarkQueryFinished() {
		auto expected = QuackQueryState::ACTIVE;
		query_state.compare_exchange_strong(expected, QuackQueryState::FINISHED);
	}

	QuackStatementState statement;
};

struct QuackConnectionSnapshot {
	string server_id;
	string connection_id;
	string client_id_hash;
	string sql_query;
	QuackQueryState query_state = QuackQueryState::IDLE;
	timestamp_t query_started_at {0};
	//! Rows in the connection's result cache
	optional_idx cached_rows;
};

struct CacheExpiryEntry {
	timestamp_t served_at;
	string connection_id;
};

struct CacheExpiresLater {
	bool operator()(const CacheExpiryEntry &lhs, const CacheExpiryEntry &rhs) const {
		return lhs.served_at > rhs.served_at;
	}
};

class QuackServer {
public:
	explicit QuackServer(ClientContext &context_p, const QuackUri &uri_p, const string &token_p);
	virtual ~QuackServer();

	shared_ptr<QuackConnection> GetConnection(const string &connection_id);
	string CreateNewConnection(const string &connection_id, const string &client_id_hash,
	                           idx_t heartbeat_timeout_seconds);
	bool DisconnectConnection(const string &connection_id);

	string GenerateConnectionId();

	//! Throw InvalidInputException if `token` doesn't meet requirements(currently, length >= 4)
	static void ValidateToken(const string &token);

	vector<QuackConnectionSnapshot> GetActiveConnectionSnap();

	const string &Token() {
		return token;
	}

	const QuackUri &ListenUri() const {
		return uri;
	}

protected:
	//! Handle one request, turning any exception raised along the way into an ErrorResponse
	unique_ptr<QuackMessage> HandleMessage(MemoryStream &read_stream);
	unique_ptr<QuackMessage> DispatchMessage(MemoryStream &read_stream);
	//! Drops caches idle past quack_result_ttl
	void SweepExpiredCaches(DatabaseInstance &db);
	//! Give `connection` its expiry-queue slot when its cache is created; caller must hold connection.lock.
	void RegisterCacheForExpiry(QuackConnection &connection);
	unique_ptr<QuackMessage> HandleMessageInternal(DatabaseInstance &db, QuackMessage &received_message,
	                                               optional_ptr<QuackConnection> connection);

protected:
	weak_ptr<DatabaseInstance> db_ptr;
	mutex active_connections_mutex;
	unordered_map<string, shared_ptr<QuackConnection>> active_connections;
	shared_ptr<atomic<idx_t>> live_caches = make_shared_ptr<atomic<idx_t>>(0);
	//! Min-heap of the expiry-queue slots, at most one per connection with a cache
	mutex cache_expiry_mutex;
	std::priority_queue<CacheExpiryEntry, vector<CacheExpiryEntry>, CacheExpiresLater> cache_expiry_queue;

	QuackUri uri;

private:
	bool RenewConnectionLease(const string &connection_id, const shared_ptr<QuackConnection> &connection);
	static void CleanupExpiredConnection(QuackConnection &connection);
	//! Destroys reaped caches on a one-shot detached thread so no response waits on their teardown
	void DestroyCachesDetached(vector<unique_ptr<QuackResultCache>> doomed);

	string token;
	//! Per-server random key that seeds the HMAC for client_id_hash.
	string server_hmac_key;
};

} // namespace duckdb

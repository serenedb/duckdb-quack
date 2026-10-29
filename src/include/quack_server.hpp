#pragma once

#include <queue>

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/common/serializer/read_stream.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "quack_claim_buffer.hpp"
#include "quack_fetch_collector.hpp"
#include "quack_message.hpp"
#include "quack_result_cache.hpp"
#include "quack_statement.hpp"

namespace duckdb {

//! A session of the host that embeds the quack server: the context its statements run on, and the driver
//! that runs them.
class QuackHostSession {
public:
	virtual ~QuackHostSession() = default;

	virtual ClientContext &Context() = 0;
	//! Drive `runner` with Step until it finishes, after the statements handed over before it. Must not
	//! wait for the statement.
	virtual void Run(unique_ptr<QuackStatementRunner> runner) = 0;
	//! Interrupt the running statement, and make its driver look at it again.
	virtual void Interrupt() = 0;
};

//! What the host gives one request: the session a CONNECTION opens, after it checked the credentials.
class QuackServerHost {
public:
	virtual ~QuackServerHost() = default;

	//! Throw to refuse the connection: the client gets the exception.
	virtual unique_ptr<QuackHostSession> OpenSession(const string &connection_id,
	                                                 const ConnectionRequestMessage &request) = 0;
};

enum class QuackQueryState : uint8_t { IDLE, ACTIVE, FINISHED, CANCELLED };

//! One connection of a server, as quack_active_connections() lists it
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

//! The stream the connection's active query fills, one at a time.
struct QuackStatementState {
	annotated_mutex lock;
	shared_ptr<QuackResultStream> stream DUCKDB_GUARDED_BY(lock);
	hugeint_t query_uuid DUCKDB_GUARDED_BY(lock) = 0;
	//! A late FETCH for this query_uuid gets this error. The stream and its payloads can then go away.
	ErrorData abort_error DUCKDB_GUARDED_BY(lock);
};

struct QuackConnection {
	QuackConnection(string connection_id_p, idx_t heartbeat_timeout_seconds_p,
	                unique_ptr<QuackHostSession> host_session_p);
	~QuackConnection();

	//! Renew unless the timeout has already elapsed. Once expired, a lease cannot be revived.
	bool TryRenewLease();
	//! True if the lease timeout has elapsed, latching the expiry ("cannot be revived").
	bool LeaseExpired();
	bool LeaseExpiredLocked(time_point<steady_clock> now) DUCKDB_REQUIRES(lease_lock);

	//! Guards the session state below and the result cache. Never held across a statement.
	mutex lock;
	unique_ptr<QuackHostSession> host_session;
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

	//! The client connected with QUACK_SERENEDB_HEADER: the responses may carry the SereneDB fields
	bool serenedb_client = false;

	//! Stable per-client reconnect key: HMAC-SHA256(server_hmac_key, client_id). Intentionally excludes
	//! connection_id so it stays identical across (re)connections for the same client_id. Empty if no client_id.
	string client_id_hash;

	//! Heartbeat and lease variables
	const idx_t heartbeat_timeout_seconds;
	annotated_mutex lease_lock;
	time_point<steady_clock> lease_last_renewed_at DUCKDB_GUARDED_BY(lease_lock);
	bool lease_expired DUCKDB_GUARDED_BY(lease_lock) = false;

	string sql_query;
	//! Read unlocked by the cache sweep, so it must be atomic.
	atomic<QuackQueryState> query_state {QuackQueryState::IDLE};
	timestamp_t query_started_at {0};

	//! Only an ACTIVE query can finish, so a concurrent CANCEL is not overwritten
	void MarkQueryFinished() {
		auto expected = QuackQueryState::ACTIVE;
		query_state.compare_exchange_strong(expected, QuackQueryState::FINISHED);
	}

	QuackStatementState statement;
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

//! One request: its body, where its handling stands while it waits on a statement, and the response.
struct QuackRequest {
	//! `body` reads the request bytes and must outlive the request. `serenedb_client`: the request carries
	//! QUACK_SERENEDB_HEADER.
	QuackRequest(ReadStream &body, bool serenedb_client) : body(body), serenedb_client(serenedb_client) {
	}

	//! A PREPARE answers with the rows it has instead of waiting for quack_prepare_inline_rows, so a long
	//! statement continues under FETCH, where the client can interrupt it.
	void AnswerEarly() {
		answer_early = true;
	}

	//! The request's type, once QuackServer::Handle read it
	MessageType Type() const {
		return header.type;
	}

	//! The response body, once QuackServer::Handle answered the request
	const_data_ptr_t ResponseData() const {
		return response_data;
	}
	idx_t ResponseSize() const {
		return response_size;
	}

	enum class Phase : uint8_t { DECODE, START, WAIT_BOUND, DRAIN, FETCH, SEND, DONE };

	ReadStream &body;
	bool serenedb_client;
	Phase phase = Phase::DECODE;
	MessageHeader header {MessageType::INVALID, string()};
	unique_ptr<QuackMessage> message;
	shared_ptr<QuackConnection> connection;
	int64_t start_time = 0;
	//! Set while the request's waker is registered and has not fired
	shared_ptr<atomic<bool>> waker_armed = make_shared_ptr<atomic<bool>>(false);

	//! PREPARE: the statement's stream and what the inline drain took from it
	shared_ptr<QuackResultStream> stream;
	bool answer_early = false;
	bool retain_result = false;
	idx_t max_inline_rows = 0;
	idx_t inline_rows = 0;
	idx_t consumed = 0;
	vector<unique_ptr<DataChunkWrapper>> results;

	//! FETCH: the dense index the request names
	idx_t dense_index = 0;

	unique_ptr<QuackMessage> response;
	MemoryStream response_stream;
	const_data_ptr_t response_data = nullptr;
	idx_t response_size = 0;
};

//! The quack protocol of a server. The host that embeds it brings the transport, the sessions and the
//! threads: a request that has to wait on a statement returns to the host instead of blocking.
class QuackServer {
public:
	//! `server_id` names the server in quack_active_connections()
	QuackServer(DatabaseInstance &db, string server_id);
	~QuackServer();

	//! Answer `request`, or return false while it waits on a statement: `waker` then fires once, from any
	//! thread, when it may go on, and the host calls Handle again.
	bool Handle(QuackRequest &request, QuackServerHost &host, const QuackWaker &waker);
	//! Drop the sessions whose lease ran out, and the result caches idle past quack_result_ttl.
	void Sweep();
	//! Close the session, as a DISCONNECT does.
	bool Disconnect(const string &connection_id);
	//! Close every session.
	void DisconnectAll();
	vector<QuackConnectionSnapshot> GetActiveConnectionSnap();

private:
	unique_ptr<QuackMessage> Dispatch(QuackRequest &request, QuackServerHost &host, const QuackWaker &waker);
	unique_ptr<QuackMessage> Connect(QuackRequest &request, QuackServerHost &host);
	unique_ptr<QuackMessage> Prepare(QuackRequest &request, const QuackWaker &waker);
	unique_ptr<QuackMessage> Fetch(QuackRequest &request, const QuackWaker &waker);
	unique_ptr<QuackMessage> SendData(QuackRequest &request, const QuackWaker &waker);
	unique_ptr<QuackMessage> Cancel(QuackRequest &request);
	unique_ptr<QuackMessage> Acknowledge(QuackRequest &request);
	void Finish(QuackRequest &request, unique_ptr<QuackMessage> response);
	void LogRequest(QuackRequest &request);

	shared_ptr<QuackConnection> GetConnection(const string &connection_id);
	bool RenewConnectionLease(const string &connection_id, const shared_ptr<QuackConnection> &connection);
	//! Drops caches idle past quack_result_ttl
	void SweepExpiredCaches(DatabaseInstance &db);
	//! Give `connection` its expiry-queue slot when its cache is created; caller must hold connection.lock.
	void RegisterCacheForExpiry(QuackConnection &connection);
	string GenerateConnectionId();

	weak_ptr<DatabaseInstance> db_ptr;
	string server_id;
	mutex active_connections_mutex;
	unordered_map<string, shared_ptr<QuackConnection>> active_connections;
	shared_ptr<atomic<idx_t>> live_caches = make_shared_ptr<atomic<idx_t>>(0);
	//! Min-heap of the expiry-queue slots, at most one per connection with a cache
	mutex cache_expiry_mutex;
	std::priority_queue<CacheExpiryEntry, vector<CacheExpiryEntry>, CacheExpiresLater> cache_expiry_queue;
	//! Per-server random key that seeds the HMAC for client_id_hash.
	string server_hmac_key;
};

} // namespace duckdb

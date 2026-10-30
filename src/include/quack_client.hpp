#pragma once

#include "duckdb/common/http_util.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

#include "quack_message.hpp"
#include "quack_log.hpp"
#include "quack_periodic_worker.hpp"
#include "quack_uri.hpp"

namespace duckdb {
class QuackClientConnection;
struct QuackClientWrapper;

//! The active query's id, for log correlation. Invalid when no query runs (e.g. transaction start).
optional_idx QuackActiveClientQueryId(ClientContext &context);

//! How long to wait between two heartbeats of a lease of `heartbeat_timeout_seconds`, with jitter
milliseconds QuackHeartbeatInterval(idx_t heartbeat_timeout_seconds, RandomEngine &random);

//! The deadline of a heartbeat, a cancel or a disconnect: none waits for a statement, so a peer that does not
//! answer in this time is gone
optional_idx QuackControlDeadline();

//! The deadline of a request of the statement `context` runs on `connection`: the statement's own one, or for a
//! statement without one sent to a SereneDB server, quack_default_statement_timeout from now
optional_idx QuackStatementDeadline(ClientContext &context, optional_ptr<const QuackClientConnection> connection);

class QuackClient {
public:
	explicit QuackClient(DatabaseInstance &db_p, const QuackUri &uri_p);
	virtual ~QuackClient();

	template <class TARGET>
	unique_ptr<TARGET> Request(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message) {
		return Request<TARGET>(context, std::move(request_message),
		                       context ? QuackStatementDeadline(*context, owner_connection) : optional_idx());
	}

	//! A request still running at `deadline` (a TimePoint::Now() tick, as ClientContext::query_deadline) fails
	template <class TARGET>
	unique_ptr<TARGET> Request(optional_ptr<ClientContext> context, unique_ptr<QuackMessage> request_message,
	                           optional_idx deadline) {
		auto response_message = RequestInternal(context, std::move(request_message), deadline);
		if (response_message->Type() != TARGET::TYPE) {
			if (response_message->Type() == MessageType::ERROR_RESPONSE) {
				// if we get an error throw it immediately
				NoteError(response_message->Cast<ErrorResponse>());
				response_message->Cast<ErrorResponse>().Error().Throw();
			}
			throw IOException("Expected %s message, got %s instead", MessageTypeToString(TARGET::TYPE),
			                  MessageTypeToString(response_message->Type()));
		}
		return unique_ptr_cast<QuackMessage, TARGET>(std::move(response_message));
	}

	//! Invalidate the owning connection if the error says the server's database died. Call before rethrowing.
	void NoteError(const ErrorResponse &error_response);

	//! The connection this client is checked out from - set by QuackClientWrapper. Null for the initial connect
	//! and for a one-shot quack_query.
	void SetOwnerConnection(optional_ptr<const QuackClientConnection> owner_connection_p) {
		owner_connection = owner_connection_p;
	}

	//! POST already-serialized request bytes and return the raw response body, throwing on transport failure.
	//! Lets an async sender serialize on a producer thread and perform the blocking POST from the ASYNC pool;
	//! pass context=nullptr when called off the execution thread (parameters fall back to the database).
	//! A request still running at `deadline`, the statement's ClientContext::query_deadline, fails with the
	//! statement's timeout error.
	virtual string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size,
	                       optional_idx deadline) = 0;

	//! Encode a request (inject client_query_id when a query is active, then serialize) into `out`.
	//! Protocol-level and lock-free; the caller owns `out` and any locking around it.
	static void EncodeRequest(optional_ptr<ClientContext> context, QuackMessage &message, MemoryStream &out,
	                          bool serenedb_peer);

	//! Decode a response body (owned bytes) into a message. Protocol-level and lock-free.
	static unique_ptr<QuackMessage> DecodeResponse(const string &response_body);

	//! Emit a Quack request log entry through `logger`. Pass the query's context logger for per-query
	//! attribution (connection/transaction/query id columns); the db logger only fits context-less
	//! teardown. Safe from an async pool thread when handed a captured context logger.
	void LogRequest(Logger &logger, MessageType request_type, const string &connection_id, optional_idx client_query_id,
	                const string &query, int64_t duration_ms, MessageType response_type, const string &error);

	//! Stamp the logger for this client's HTTP transport-log entries. Set at checkout so a pooled client
	//! (incl. an async send whose pool thread has no ClientContext) logs under the checking-out query.
	void SetRequestLogger(shared_ptr<Logger> logger);

	static unique_ptr<QuackClient> GetClient(DatabaseInstance &db, const QuackUri &uri);
	static unique_ptr<QuackClient> GetClient(ClientContext &context, const QuackUri &uri);

	//! The token to connect with: `token`, or the one of the quack secret scoped to `uri`. Throws if there is none.
	static string ResolveToken(ClientContext &context, const QuackUri &uri, string token);

	static shared_ptr<QuackClientConnection> ConnectToServer(ClientContext &context, const QuackUri &uri, string token,
	                                                         string client_id, idx_t heartbeat_timeout_seconds);

	//! Resolve the effective client_id for a new connection
	static string ResolveClientId(ClientContext &context, optional_ptr<const Value> explicit_value);
	//! Resolve the requested heartbeat lease timeout for a new connection
	static idx_t ResolveHeartbeatTimeout(ClientContext &context, optional_ptr<const Value> explicit_value);

	//! Throw unless `client_id` is either empty ("no client_id") or >= 4 characters
	static void ValidateClientId(const string &client_id);
	//! Heartbeat leases must have a positive timeout.
	static void ValidateHeartbeatTimeout(idx_t heartbeat_timeout_seconds);

protected:
	//! Resolve the logger for a request: the context (per-query) logger when available, else the db logger.
	Logger &GetRequestLogger(optional_ptr<ClientContext> context);

	mutex request_mutex;
	MemoryStream write_stream;
	DatabaseInstance &db;
	QuackUri uri;
	//! HTTP transport-log logger, stamped at checkout (see SetRequestLogger).
	shared_ptr<Logger> request_logger;
	//! See SetOwnerConnection
	optional_ptr<const QuackClientConnection> owner_connection;

private:
	virtual unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                                 unique_ptr<QuackMessage> request_message,
	                                                 optional_idx deadline) = 0;
};

class QuackClientConnection : public enable_shared_from_this<QuackClientConnection> {
public:
	explicit QuackClientConnection(DatabaseInstance &db_p, unique_ptr<QuackClient> client_p, QuackUri uri_p,
	                               string connection_id_p, idx_t quack_version_p, bool serenedb_peer_p,
	                               idx_t heartbeat_timeout_seconds_p, idx_t max_connections_cached);
	~QuackClientConnection();

	void CancelQuery(hugeint_t query_uuid) const;
	//! CancelQuery off the calling thread, for a caller that does not wait for the answer
	void CancelQueryInBackground(hugeint_t query_uuid) const;
	//! End the session on the server. A closed connection refuses every later request.
	void Close();

	//! The PREPARE of `sql` on this connection
	unique_ptr<PrepareRequestMessage> MakePrepare(string sql, hugeint_t query_uuid) const;
	//! Send `prepare`. One that fails, such as at the statement's deadline, can leave its statement running on the
	//! server, so it cancels the statement.
	unique_ptr<PrepareResponseMessage> Prepare(ClientContext &context, unique_ptr<PrepareRequestMessage> prepare) const;
	//! The PREPAREs made on this connection. Each one stops the statement the server ran before it.
	idx_t StatementsStarted() const {
		return statements_started;
	}

	//! Keep the lease alive from a thread of this connection's own. A connection of an attached database is
	//! kept alive by the database instead.
	void StartHeartbeat();
	void SendHeartbeat() const;

	const string &ConnectionId() const {
		return connection_id;
	}
	const QuackUri &ServerURI() const {
		return uri;
	}
	idx_t QuackVersion() const {
		return quack_version;
	}
	//! The server is a SereneDB one: the requests may carry the SereneDB fields (see QUACK_SERENEDB_HEADER)
	bool SereneDBPeer() const {
		return serenedb_peer;
	}
	idx_t HeartbeatTimeoutSeconds() const {
		return heartbeat_timeout_seconds;
	}

	//! Get a client (either a cached one, or open a new one if required).
	//! Throws once the server has told us its database is gone - see MarkServerInvalidated.
	unique_ptr<QuackClientWrapper> GetClient(ClientContext &context) const;
	//! Return a client back to the cache
	void StoreClient(unique_ptr<QuackClient> client_p) const;

	//! The server's database is invalidated, so this connection is a dead end - fail fast on it. Reconnecting
	//! would be worse than useless: an invalidated server cannot run its authentication query either, so it
	//! rejects every reconnect with a misleading "Authentication failed".
	void MarkServerInvalidated() const;
	bool ServerInvalidated() const {
		return server_invalidated;
	}

	//! The server no longer knows this session: its lease ran out, or the server restarted
	void MarkLost() const {
		lost = true;
	}
	bool Lost() const {
		return lost;
	}
	bool Closed() const {
		return closed;
	}

private:
	friend class QuackClient;

	unique_ptr<QuackClient> TakeClient(optional_ptr<ClientContext> context) const;

	DatabaseInstance &db;
	QuackUri uri;
	string connection_id;
	//! The protocol version agreed during the connection handshake
	idx_t quack_version;
	bool serenedb_peer;
	//! Lease timeout accepted by the server during the connection handshake.
	idx_t heartbeat_timeout_seconds;
	mutable mutex lock;
	mutable atomic<bool> server_invalidated {false};
	mutable atomic<bool> lost {false};
	mutable atomic<idx_t> statements_started {0};
	atomic<bool> closed {false};
	//! Bounds cached_clients: each cached client holds a persistent socket that pins a server
	//! connection slot, so an unbounded cache would let one attach starve the server's budget.
	idx_t max_connections_cached;
	mutable vector<unique_ptr<QuackClient>> cached_clients;

	QuackPeriodicWorker heartbeat;
};

struct QuackClientWrapper {
	QuackClientWrapper(unique_ptr<QuackClient> client, shared_ptr<const QuackClientConnection> client_connection);
	~QuackClientWrapper();

	QuackClient &GetClient();

private:
	unique_ptr<QuackClient> client;
	shared_ptr<const QuackClientConnection> client_connection;
};

class HttpsQuackClient : public QuackClient {
public:
	static constexpr uint64_t HTTP_TIMEOUT_SECONDS = 86400;

	HttpsQuackClient(DatabaseInstance &db, const QuackUri &uri_p);
	~HttpsQuackClient() override;

	string PostRaw(optional_ptr<ClientContext> context, const_data_ptr_t data, idx_t size,
	               optional_idx deadline) override;

private:
	unique_ptr<QuackMessage> RequestInternal(optional_ptr<ClientContext> context,
	                                         unique_ptr<QuackMessage> request_message, optional_idx deadline) override;
	//! POST bytes assuming request_mutex is already held. A CONNECTION_REQUEST carries QUACK_SERENEDB_HEADER.
	string PostRawLocked(const_data_ptr_t data, idx_t size, optional_idx deadline, MessageType type);
	//! Lazily initialise http_params (context-aware) and attach request_logger; assumes request_mutex is held.
	void EnsureHttpParams(optional_ptr<ClientContext> context);

private:
	unique_ptr<HTTPParams> http_params;
	//! Extra HTTP headers resolved once from the `quack` secret (EXTRA_HTTP_HEADERS),
	//! injected into every request. Loaded lazily alongside http_params.
	HTTPHeaders extra_headers;
	//! Persistent keep-alive HTTP client: reused across requests so the TCP connection (and its
	//! warm congestion window) survives between POSTs; replaced by the retry path on dead sockets.
	unique_ptr<HTTPClient> http_client;
};

} // namespace duckdb

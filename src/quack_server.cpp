#include "duckdb/common/serializer/binary_deserializer.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/valid_checker.hpp"

#include "quack_server.hpp"
#include "quack_fetch_collector.hpp"
#include "quack_log.hpp"
#include "quack_random.hpp"
#include "quack_session_state.hpp"
#include "quack_settings.hpp"
#include "quack_statement.hpp"
#include "quack_storage.hpp"

#include "mbedtls_wrapper.hpp"

#include <absl/cleanup/cleanup.h>

namespace duckdb {

static bool LeaseTimeoutElapsed(time_point<steady_clock> last_renewed_at, time_point<steady_clock> now,
                                idx_t timeout_seconds) {
	if (now <= last_renewed_at) {
		return false;
	}
	auto elapsed_seconds = duration_cast<std::chrono::seconds>(now - last_renewed_at).count();
	return static_cast<idx_t>(elapsed_seconds) >= timeout_seconds;
}

//! Stop the connection's statement if `matches` accepts its stream and query: error its stream, so the requests
//! that wait on it and the producers parked on its capacity wake, and interrupt it. The host's driver then
//! finishes it.
template <class MATCHES>
static bool AbortStatementIf(QuackConnection &connection, const string &reason, MATCHES &&matches) {
	shared_ptr<QuackResultStream> stream;
	{
		annotated_lock_guard<annotated_mutex> guard(connection.statement.lock);
		if (!matches(connection.statement.stream.get(), connection.statement.query_uuid)) {
			return false;
		}
		stream = std::move(connection.statement.stream);
		if (stream) {
			// the abort reason must not hide a real failure
			connection.statement.abort_error =
			    stream->buffer.HasError() ? stream->buffer.GetError() : ErrorData(ExceptionType::INTERRUPT, reason);
		}
	}
	// The statement can hold a scan that waits for a client batch. Error those streams too.
	if (auto session_state = QuackSessionState::Get(connection.host_session->Context())) {
		session_state->Streams().Clear(reason);
	}
	if (!stream) {
		return true;
	}
	auto was_finished = stream->buffer.Finished();
	stream->buffer.SetError(ErrorData(ExceptionType::INTERRUPT, reason));
	if (!was_finished) {
		connection.host_session->Interrupt();
	}
	return true;
}

static void AbortStatement(QuackConnection &connection, const string &reason) {
	AbortStatementIf(connection, reason, [](const QuackResultStream *, hugeint_t) { return true; });
}

//! Register the request's waker with `buffer` unless it waits there already. True when it was registered now:
//! the caller then looks once more, so a change between its look and the registration is not missed.
template <class BUFFER>
static bool ArmWaker(QuackRequest &request, BUFFER &buffer, const QuackWaker &waker) {
	if (request.waker_armed->exchange(true)) {
		return false;
	}
	buffer.AddWaker([armed = request.waker_armed, waker] {
		armed->store(false);
		waker();
	});
	return true;
}

QuackConnection::QuackConnection(string connection_id_p, idx_t heartbeat_timeout_seconds_p,
                                 unique_ptr<QuackHostSession> host_session_p)
    : host_session(std::move(host_session_p)), connection_id(std::move(connection_id_p)),
      heartbeat_timeout_seconds(heartbeat_timeout_seconds_p), lease_last_renewed_at(steady_clock::now()) {
}

QuackConnection::~QuackConnection() {
	AbortStatement(*this, "connection closed during fetch");
}

bool QuackConnection::LeaseExpiredLocked(time_point<steady_clock> now) {
	if (!lease_expired && LeaseTimeoutElapsed(lease_last_renewed_at, now, heartbeat_timeout_seconds)) {
		lease_expired = true;
	}
	return lease_expired;
}

bool QuackConnection::LeaseExpired() {
	annotated_lock_guard<annotated_mutex> guard(lease_lock);
	return LeaseExpiredLocked(steady_clock::now());
}

bool QuackConnection::TryRenewLease() {
	annotated_lock_guard<annotated_mutex> guard(lease_lock);
	auto now = steady_clock::now();
	if (LeaseExpiredLocked(now)) {
		return false;
	}
	lease_last_renewed_at = now;
	return true;
}

//! Update the cache with what its stream holds. Drops the cache when the result grows past
//! quack_cache_max_rows, because it is then too large to keep for a reconnect.
static void SyncResultCache(QuackConnection &connection, QuackResultStream &stream, DatabaseInstance &db) {
	lock_guard<mutex> lock(connection.lock);
	auto &cache = connection.result_cache;
	if (!cache || cache->stream.get() != &stream) {
		return;
	}
	cache->last_served_at = Timestamp::GetCurrentTimestamp();
	cache->retained_rows = stream.RetainedRows();
	auto max_cache_rows = CacheMaxRows(db);
	if (max_cache_rows != 0 && cache->retained_rows > max_cache_rows) {
		// the acks release the payloads again
		stream.DropRetention();
		connection.ClearResultCache();
		return;
	}
	connection.SyncCachedRows();
}

QuackServer::QuackServer(DatabaseInstance &db, string server_id_p)
    : db_ptr(db.shared_from_this()), server_id(std::move(server_id_p)) {
	server_hmac_key = QuackRandomToken(db);
	QuackStorageExtensionInfo::GetState(db).AddServer(*this);
}

QuackServer::~QuackServer() {
	if (auto db = db_ptr.lock()) {
		QuackStorageExtensionInfo::GetState(*db).RemoveServer(*this);
	}
	DisconnectAll();
}

vector<QuackConnectionSnapshot> QuackServer::GetActiveConnectionSnap() {
	// The query fields change under the connection's lock: copy the connections out first, so the two locks
	// are never held together
	vector<shared_ptr<QuackConnection>> connections;
	{
		lock_guard<mutex> guard(active_connections_mutex);
		connections.reserve(active_connections.size());
		for (auto &entry : active_connections) {
			connections.push_back(entry.second);
		}
	}
	vector<QuackConnectionSnapshot> result;
	result.reserve(connections.size());
	for (auto &connection : connections) {
		QuackConnectionSnapshot snapshot;
		snapshot.server_id = server_id;
		snapshot.connection_id = connection->connection_id;
		snapshot.client_id_hash = connection->client_id_hash;
		{
			lock_guard<mutex> guard(connection->lock);
			snapshot.sql_query = connection->sql_query;
			snapshot.query_state = connection->query_state;
			snapshot.query_started_at = connection->query_started_at;
		}
		auto cached_rows = connection->cached_rows.load();
		if (cached_rows != DConstants::INVALID_INDEX) {
			snapshot.cached_rows = cached_rows;
		}
		result.push_back(std::move(snapshot));
	}
	return result;
}

bool QuackServer::RenewConnectionLease(const string &connection_id, const shared_ptr<QuackConnection> &connection) {
	shared_ptr<QuackConnection> expired_connection;
	{
		lock_guard<mutex> guard(active_connections_mutex);
		auto entry = active_connections.find(connection_id);
		if (entry == active_connections.end() || entry->second.get() != connection.get()) {
			return false;
		}
		if (connection->TryRenewLease()) {
			return true;
		}
		expired_connection = std::move(entry->second);
		active_connections.erase(entry);
	}
	AbortStatement(*expired_connection, "connection heartbeat lease expired");
	return false;
}

void QuackServer::RegisterCacheForExpiry(QuackConnection &connection) {
	if (connection.cache_in_expiry_queue) {
		// the sweep re-queues the existing slot under the new cache's timestamp once its old snapshot pops
		return;
	}
	connection.cache_in_expiry_queue = true;
	lock_guard<mutex> lock(cache_expiry_mutex);
	cache_expiry_queue.push({connection.result_cache->last_served_at, connection.connection_id});
}

void QuackServer::SweepExpiredCaches(DatabaseInstance &db) {
	// nothing cached anywhere, we skip the settings lookup and the expiry queue
	if (live_caches->load(std::memory_order_relaxed) == 0) {
		return;
	}
	auto ttl_micros = ResultTtlMicros(db);
	if (ttl_micros == 0) {
		return;
	}
	auto now = Timestamp::GetCurrentTimestamp();
	vector<CacheExpiryEntry> candidates;
	{
		lock_guard<mutex> lock(cache_expiry_mutex);
		while (!cache_expiry_queue.empty() && now.value - cache_expiry_queue.top().served_at.value > ttl_micros) {
			candidates.push_back(cache_expiry_queue.top());
			cache_expiry_queue.pop();
		}
	}
	vector<CacheExpiryEntry> requeue;
	vector<unique_ptr<QuackResultCache>> doomed;
	vector<shared_ptr<QuackConnection>> abandoned;
	for (auto &entry : candidates) {
		auto connection = GetConnection(entry.connection_id);
		if (!connection) {
			// disconnected, the cache died with the connection and the slot dies here
			continue;
		}
		{
			unique_lock<mutex> lock(connection->lock, std::try_to_lock);
			if (!lock.owns_lock()) {
				// another handler is mutating the session state; retry on the next sweep
				requeue.push_back(std::move(entry));
				continue;
			}
			bool still_serving = false;
			auto expired = ExpireCacheIfStale(*connection, now, ttl_micros, still_serving);
			if (expired) {
				doomed.push_back(std::move(expired));
				if (still_serving) {
					abandoned.push_back(connection);
				}
			}
			if (connection->result_cache) {
				// served (or replaced) since the snapshot was queued, keep the slot under the fresh timestamp
				entry.served_at = connection->result_cache->last_served_at;
				requeue.push_back(std::move(entry));
			} else {
				connection->cache_in_expiry_queue = false;
			}
		}
	}
	for (auto &connection : abandoned) {
		// Releases the query the client walked away from. A later FETCH finds this error.
		AbortStatement(*connection, "Query was interrupted");
	}
	if (!requeue.empty()) {
		lock_guard<mutex> lock(cache_expiry_mutex);
		for (auto &entry : requeue) {
			cache_expiry_queue.push(std::move(entry));
		}
	}
}

void QuackServer::Sweep() {
	auto db = db_ptr.lock();
	if (!db) {
		return;
	}
	vector<shared_ptr<QuackConnection>> expired;
	{
		lock_guard<mutex> guard(active_connections_mutex);
		for (auto entry = active_connections.begin(); entry != active_connections.end();) {
			if (entry->second->LeaseExpired()) {
				expired.push_back(std::move(entry->second));
				active_connections.erase(entry++);
			} else {
				++entry;
			}
		}
	}
	for (auto &connection : expired) {
		AbortStatement(*connection, "connection heartbeat lease expired");
	}
	SweepExpiredCaches(*db);
}

shared_ptr<QuackConnection> QuackServer::GetConnection(const string &connection_id) {
	lock_guard<mutex> lock(active_connections_mutex);
	auto it = active_connections.find(connection_id);
	if (it != active_connections.end()) {
		return it->second;
	}
	return nullptr;
}

bool QuackServer::Disconnect(const string &connection_id) {
	shared_ptr<QuackConnection> connection;
	{
		lock_guard<mutex> lock(active_connections_mutex);
		auto entry = active_connections.find(connection_id);
		if (entry == active_connections.end()) {
			return false;
		}
		connection = std::move(entry->second);
		active_connections.erase(entry);
	}
	AbortStatement(*connection, "connection closed during fetch");
	return true;
}

void QuackServer::DisconnectAll() {
	unordered_map<string, shared_ptr<QuackConnection>> closed;
	{
		lock_guard<mutex> lock(active_connections_mutex);
		closed = std::move(active_connections);
		active_connections.clear();
	}
	for (auto &entry : closed) {
		AbortStatement(*entry.second, "the server is shutting down");
	}
}

// Derive a stable, per-client reconnect identifier as HMAC-SHA256(server_hmac_key, client_id)
static string ComputeClientHash(const string &server_hmac_key, const string &client_id) {
	unsigned char digest[duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES];
	duckdb_mbedtls::MbedTlsWrapper::Hmac256(server_hmac_key.data(), server_hmac_key.size(), client_id.data(),
	                                        client_id.size(), reinterpret_cast<char *>(digest));
	return QuackHexEncode(digest, duckdb_mbedtls::MbedTlsWrapper::SHA256_HASH_LENGTH_BYTES);
}

string QuackServer::GenerateConnectionId() {
	auto db = db_ptr.lock();
	if (!db) {
		throw InternalException("Database was closed");
	}
	return QuackRandomToken(*db);
}

static bool ServerSupportsMessage(MessageType type) {
	switch (type) {
	case MessageType::CONNECTION_REQUEST:
	case MessageType::PREPARE_REQUEST:
	case MessageType::FETCH_REQUEST:
	case MessageType::SEND_DATA_REQUEST:
	case MessageType::DISCONNECT_MESSAGE:
	case MessageType::CANCEL_REQUEST:
	case MessageType::ACKNOWLEDGEMENT:
	case MessageType::HEARTBEAT_REQUEST:
		return true;
	default:
		return false;
	}
}

bool QuackServer::Handle(QuackRequest &request, QuackServerHost &host, const QuackWaker &waker) {
	unique_ptr<QuackMessage> response;
	try {
		response = Dispatch(request, host, waker);
	} catch (std::exception &ex) {
		// The client reports a transport failure as "Failed to send message", so an error has to be a
		// response instead.
		response = make_uniq<ErrorResponse>(ErrorData(ex));
	} catch (...) {
		response = make_uniq<ErrorResponse>("Unknown error while handling request");
	}
	if (!response) {
		return false;
	}
	Finish(request, std::move(response));
	return true;
}

void QuackServer::Finish(QuackRequest &request, unique_ptr<QuackMessage> response) {
	if (response->Type() == MessageType::ERROR_RESPONSE) {
		// tell the client whether we are still usable - ask the database rather than guess from the exception
		// type, it is the one that decides it has been invalidated
		auto db = db_ptr.lock();
		response->Cast<ErrorResponse>().SetMustInvalidate(!db || ValidChecker::IsInvalidated(*db));
	}
	request.phase = QuackRequest::Phase::DONE;
	request.response = std::move(response);
	LogRequest(request);
	if (auto raw = request.response->RawPayload()) {
		// already serialized: the bytes go out as they are
		auto body_start = request.response->RawPayloadStart();
		request.response_data = raw->GetData() + body_start;
		request.response_size = raw->GetPosition() - body_start;
		return;
	}
	request.response->ToMemoryStream(request.response_stream);
	request.response_data = request.response_stream.GetData();
	request.response_size = request.response_stream.GetPosition();
}

void QuackServer::LogRequest(QuackRequest &request) {
	auto db = db_ptr.lock();
	if (!db || !request.message) {
		return;
	}
	auto &logger = Logger::Get(*db);
	if (!logger.ShouldLog(QuackLogType::NAME, QuackLogType::LEVEL)) {
		return;
	}
	auto duration_ms = QuackNowMillis() - request.start_time;
	string error;
	if (request.response->Type() == MessageType::ERROR_RESPONSE) {
		error = request.response->Cast<ErrorResponse>().ErrorMessage();
	}
	auto client_id_hash = request.connection ? request.connection->client_id_hash : string();
	auto msg = QuackLogType::ConstructLogMessage(request.header.type, request.header.connection_id, client_id_hash,
	                                             request.header.client_query_id, request.message->LoggableQuery(), "",
	                                             duration_ms, request.response->Type(), error);
	logger.WriteLog(QuackLogType::NAME, QuackLogType::LEVEL, msg);
}

unique_ptr<QuackMessage> QuackServer::Dispatch(QuackRequest &request, QuackServerHost &host, const QuackWaker &waker) {
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
	}
	if (request.phase == QuackRequest::Phase::DECODE) {
		if (ValidChecker::IsInvalidated(*db)) {
			// bail out before touching the database: a client reconnecting to an invalidated server would
			// otherwise be told its token was rejected
			return make_uniq<ErrorResponse>("The server database has been invalidated and the server must be "
			                                "restarted before it can be used again. Original error: \"%s\"",
			                                ValidChecker::Get(*db).InvalidatedMessage());
		}
		request.start_time = QuackNowMillis();
		BinaryDeserializer deserializer(request.body);
		request.header = QuackMessage::DeserializeHeader(deserializer);
		// validate if the server can handle this type of message - the server cannot handle all message types
		if (!ServerSupportsMessage(request.header.type)) {
			return make_uniq<ErrorResponse>("Unsupported message type for server");
		}
		// every message but the connect request belongs to a connection
		if (request.header.type != MessageType::CONNECTION_REQUEST) {
			request.connection = GetConnection(request.header.connection_id);
			if (!request.connection) {
				return make_uniq<ErrorResponse>(QUACK_INVALID_CONNECTION_ID);
			}
		}
		SweepExpiredCaches(*db);
		// now deserialize the actual message, and any raw chunk blob that follows it
		request.message = QuackMessage::DeserializeMessage(deserializer, request.header);
		// Only supported, structurally valid messages for an existing session renew its lease.
		if (request.connection && !RenewConnectionLease(request.header.connection_id, request.connection)) {
			return make_uniq<ErrorResponse>("Connection heartbeat lease expired");
		}
		request.phase = QuackRequest::Phase::START;
	}

	switch (request.message->Type()) {
	case MessageType::CONNECTION_REQUEST:
		return Connect(request, host);
	case MessageType::DISCONNECT_MESSAGE:
		if (!Disconnect(request.connection->connection_id)) {
			return make_uniq<ErrorResponse>("Connection does not exist / already disconnected");
		}
		return make_uniq<SuccessResponse>();
	case MessageType::PREPARE_REQUEST:
		return Prepare(request, waker);
	case MessageType::FETCH_REQUEST:
		return Fetch(request, waker);
	case MessageType::SEND_DATA_REQUEST:
		return SendData(request, waker);
	case MessageType::CANCEL_REQUEST:
		return Cancel(request);
	case MessageType::ACKNOWLEDGEMENT:
		return Acknowledge(request);
	case MessageType::HEARTBEAT_REQUEST:
		return make_uniq<SuccessResponse>();
	default:
		return make_uniq<ErrorResponse>(
		    StringUtil::Format("Unimplemented message type %s", MessageTypeToString(request.message->Type())));
	}
}

unique_ptr<QuackMessage> QuackServer::Connect(QuackRequest &request, QuackServerHost &host) {
	auto &connection_request_message = request.message->Cast<ConnectionRequestMessage>();
	// Speak the newest version both sides know; reject when the client's [min, max] range misses ours.
	auto quack_version = MinValue(connection_request_message.MaximumSupportedQuackVersion(), QUACK_VERSION);
	if (connection_request_message.MinimumSupportedQuackVersion() > quack_version ||
	    quack_version < QUACK_MIN_VERSION) {
		return make_uniq<ErrorResponse>(
		    StringUtil::Format("Unsupported Quack version - server only supports versions %llu to %llu of quack",
		                       QUACK_MIN_VERSION, QUACK_VERSION));
	}
	auto heartbeat_timeout_seconds = connection_request_message.HeartbeatTimeoutSeconds();
	if (heartbeat_timeout_seconds == 0 || heartbeat_timeout_seconds > MAX_HEARTBEAT_TIMEOUT_SECONDS) {
		return make_uniq<ErrorResponse>(StringUtil::Format(
		    "heartbeat_timeout out of range - must be between 1 and %llu seconds", MAX_HEARTBEAT_TIMEOUT_SECONDS));
	}
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
	}
	heartbeat_timeout_seconds =
	    MinValue(heartbeat_timeout_seconds,
	             QuackGetUBigintSetting(*db, "quack_max_heartbeat_timeout", QUACK_MAX_HEARTBEAT_TIMEOUT_DEFAULT));
	auto max_sessions = QuackGetUBigintSetting(*db, "quack_max_sessions", 0);
	{
		lock_guard<mutex> lock(active_connections_mutex);
		if (max_sessions != 0 && active_connections.size() + opening_connections >= max_sessions) {
			return make_uniq<ErrorResponse>(
			    StringUtil::Format("Too many Quack sessions: the server keeps at most %llu", max_sessions));
		}
		opening_connections++;
	}
	bool registered = false;
	absl::Cleanup release_slot = [&] {
		if (!registered) {
			lock_guard<mutex> lock(active_connections_mutex);
			opening_connections--;
		}
	};
	auto connection_id = GenerateConnectionId();
	auto host_session = host.OpenSession(connection_id, connection_request_message);
	// scan_data_from_quack_client registers its streams here, and SEND_DATA finds them by connection.
	host_session->Context().registered_state->Insert(QuackSessionState::KEY,
	                                                 make_shared_ptr<QuackSessionState>(connection_id));
	auto connection =
	    make_shared_ptr<QuackConnection>(connection_id, heartbeat_timeout_seconds, std::move(host_session));
	connection->live_caches = live_caches;
	if (!connection_request_message.ClientId().empty()) {
		connection->client_id_hash = ComputeClientHash(server_hmac_key, connection_request_message.ClientId());
	}
	{
		lock_guard<mutex> lock(active_connections_mutex);
		D_ASSERT(active_connections.find(connection_id) == active_connections.end());
		active_connections[connection_id] = std::move(connection);
		opening_connections--;
		registered = true;
	}
	return make_uniq<ConnectionResponseMessage>(connection_id, quack_version, heartbeat_timeout_seconds);
}

unique_ptr<QuackMessage> QuackServer::Prepare(QuackRequest &request, const QuackWaker &waker) {
	auto &prepare_request_message = request.message->Cast<PrepareRequestMessage>();
	auto &connection = *request.connection;
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
	}

	auto fail = [&](const ErrorData &error) -> unique_ptr<QuackMessage> {
		// a later PREPARE may have replaced this statement already
		auto is_this_statement = [&](const QuackResultStream *current, hugeint_t) {
			return current == request.stream.get();
		};
		if (AbortStatementIf(connection, "query failed", is_this_statement)) {
			lock_guard<mutex> lock(connection.lock);
			connection.sql_query = "";
			connection.query_state = QuackQueryState::CANCELLED;
		}
		return make_uniq<ErrorResponse>(error);
	};

	if (request.phase == QuackRequest::Phase::START) {
		// Stop the previous statement first. The host runs the new one after it.
		AbortStatement(connection, "superseded by a new query");
		{
			lock_guard<mutex> lock(connection.lock);
			if (prepare_request_message.QueryUUID() != hugeint_t {0, 0}) {
				// a new client query displaces the retained one, even if caching is now off
				connection.ClearResultCache();
			}
			connection.sql_query = prepare_request_message.Query();
			connection.query_state = QuackQueryState::ACTIVE;
			connection.query_started_at = Timestamp::GetCurrentTimestamp();
		}

		request.stream = make_shared_ptr<QuackResultStream>(QuackServerStreamBufferBytes(*db));
		// Internal catalog traffic carries uuid 0. It must never displace the client's retained result.
		// A describe has no rows to keep
		bool client_query = prepare_request_message.QueryUUID() != hugeint_t {0, 0};
		request.retain_result = client_query && !prepare_request_message.DescribeOnly() && ServerCachingEnabled(*db);
		if (request.retain_result) {
			// hold every served payload, instead of releasing it once the client acks
			request.stream->RetainAll();
		}
		{
			annotated_lock_guard<annotated_mutex> guard(connection.statement.lock);
			connection.statement.stream = request.stream;
			connection.statement.query_uuid = prepare_request_message.QueryUUID();
			connection.statement.abort_error = ErrorData();
		}
		auto &context = connection.host_session->Context();
		// A statement that reads a client stream cannot bind its result before the client sends.
		// The scan finds the stream here and raises it, so PREPARE stops waiting.
		if (auto session_state = QuackSessionState::Get(context)) {
			session_state->SetStatement(request.stream);
		}
		connection.host_session->Run(make_uniq<QuackStatementRunner>(
		    context, request.stream, prepare_request_message.Query(), prepare_request_message.DescribeOnly()));

		// Put the leading batches in the PREPARE response, so a small result needs one round trip
		// only. The FETCH indices then move down by the count this drain consumed.
		// A statement that waits for client data must answer before it has a row. The client can then
		// start to send. It asks for that with inline_rows = 0.
		request.max_inline_rows =
		    prepare_request_message.InlineRows().IsValid()
		        ? prepare_request_message.InlineRows().GetIndex()
		        : QuackGetUBigintSetting(*db, "quack_prepare_inline_rows", QUACK_PREPARE_INLINE_ROWS_DEFAULT);
		request.phase = QuackRequest::Phase::WAIT_BOUND;
	}

	auto &stream = *request.stream;
	if (request.phase == QuackRequest::Phase::WAIT_BOUND) {
		while (!stream.Answerable()) {
			if (stream.buffer.Finished()) {
				// planning failed before a collector existed
				return fail(stream.buffer.HasError()
				                ? stream.buffer.GetError()
				                : ErrorData(ExceptionType::INVALID_INPUT, "Query did not return any columns"));
			}
			if (!ArmWaker(request, stream.buffer, waker)) {
				return nullptr;
			}
		}
		request.phase = QuackRequest::Phase::DRAIN;
	}

	while (request.inline_rows < request.max_inline_rows) {
		QuackFetchPayload entry;
		auto status = stream.buffer.TryPopClaimed(request.consumed + 1, entry);
		if (status == QuackClaimPopStatus::BATCH) {
			request.consumed++;
			request.inline_rows += entry.rows;
			// Batches are stored as header-less chunk blobs. Only this drain decodes them.
			MemoryStream payload_stream(entry.payload->GetData(), entry.payload_size);
			payload_stream.SetPosition(QUACK_PAYLOAD_HEADER_BYTES);
			BinaryDeserializer payload_deserializer(payload_stream);
			for (auto &chunk : DecodeQuackChunkBlob(payload_deserializer, entry.chunk_count)) {
				request.results.push_back(make_uniq<DataChunkWrapper>(*chunk));
			}
			if (request.retain_result) {
				// These rows leave in the PREPARE response, so the header for a cache replay is written here.
				FetchResponseMessage header_message;
				header_message.SetChunkCount(entry.chunk_count);
				header_message.SetBatchIndex(request.consumed);
				auto body_start = QuackPrependHeader(*entry.payload, header_message);
				stream.Retain(request.consumed, std::move(entry.payload), body_start, entry.rows);
			}
			continue;
		}
		if (status == QuackClaimPopStatus::FINISHED) {
			break;
		}
		if (status == QuackClaimPopStatus::ERRORED) {
			return fail(stream.buffer.GetError());
		}
		if (request.answer_early) {
			break;
		}
		if (!ArmWaker(request, stream.buffer, waker)) {
			return nullptr;
		}
	}
	stream.prepare_batches = request.consumed;
	if (request.retain_result) {
		// Created for every cached client query, also one with no rows. An empty result then
		// reports 0, and not "nothing cached".
		{
			lock_guard<mutex> lock(connection.lock);
			connection.result_cache =
			    make_uniq<QuackResultCache>(prepare_request_message.Query(), prepare_request_message.QueryUUID(),
			                                request.stream, connection.live_caches);
			RegisterCacheForExpiry(connection);
		}
		SyncResultCache(connection, stream, *db);
	}
	// Report more unless the drain saw a CLEAN end. A wrong true costs one more FETCH. A wrong
	// false on an errored buffer would report a truncated result as complete.
	auto needs_more_fetch = !stream.buffer.Exhausted() || stream.buffer.HasError();
	if (!needs_more_fetch) {
		connection.MarkQueryFinished();
	}
	auto response = make_uniq<PrepareResponseMessage>(stream.types, stream.names, std::move(request.results),
	                                                  needs_more_fetch, prepare_request_message.QueryUUID());
	response->SetDescribed(stream.described);
	return std::move(response);
}

unique_ptr<QuackMessage> QuackServer::Fetch(QuackRequest &request, const QuackWaker &waker) {
	auto &fetch_request_message = request.message->Cast<FetchRequestMessage>();
	auto &connection = *request.connection;
	auto db = db_ptr.lock();
	if (!db) {
		return make_uniq<ErrorResponse>("Database was closed");
	}

	if (request.phase == QuackRequest::Phase::START) {
		// FETCH touches only the buffer and the served map, so it needs neither lock on the way in.
		hugeint_t stream_uuid;
		ErrorData abort_error;
		{
			annotated_lock_guard<annotated_mutex> guard(connection.statement.lock);
			request.stream = connection.statement.stream;
			stream_uuid = connection.statement.query_uuid;
			abort_error = connection.statement.abort_error;
		}
		if (!request.stream && stream_uuid == fetch_request_message.QueryUUID() && abort_error.HasError()) {
			return make_uniq<ErrorResponse>(abort_error);
		}
		if (!request.stream || stream_uuid != fetch_request_message.QueryUUID()) {
			return make_uniq<ErrorResponse>("Result has been closed");
		}
		if (fetch_request_message.BatchIndex() == 0) {
			return make_uniq<ErrorResponse>("FETCH_REQUEST is missing its batch index");
		}

		// The request names its batch, so add back the batches PREPARE consumed inline. Served
		// payloads stay until the client acks them, so a transport retry gets the SAME batch.
		auto &stream = *request.stream;
		request.dense_index = fetch_request_message.BatchIndex() + stream.prepare_batches;
		auto dense_ack = fetch_request_message.AckIndex() + stream.prepare_batches;
		{
			lock_guard<mutex> guard(stream.serve_lock);
			// The client has everything up to the ack and cannot ask for it again. A result cache
			// keeps the payloads for longer.
			if (!stream.retain_all) {
				auto last = stream.served.upper_bound(dense_ack);
				for (auto it = stream.served.begin(); it != last; ++it) {
					stream.retained_rows -= it->second.rows;
				}
				stream.served.erase(stream.served.begin(), last);
			}
		}
		request.phase = QuackRequest::Phase::FETCH;
	}

	auto &stream = *request.stream;
	for (;;) {
		QuackClaimPopStatus status = QuackClaimPopStatus::EMPTY;
		QuackFetchPayload entry;
		shared_ptr<MemoryStream> served_batch;
		idx_t served_start = 0;
		{
			// One critical section for the check, the pop and the retain. A concurrent retry of the
			// same index cannot reach FINISHED while the first serve runs.
			lock_guard<mutex> guard(stream.serve_lock);
			auto retained = stream.served.find(request.dense_index);
			if (retained != stream.served.end()) {
				// the header with the client-visible index is already written
				served_batch = retained->second.payload;
				served_start = retained->second.body_start;
			} else {
				status = stream.buffer.TryPopClaimed(request.dense_index, entry);
				if (status == QuackClaimPopStatus::BATCH) {
					// write the header with the client-visible index, then keep the payload for a retry
					FetchResponseMessage header_message;
					header_message.SetChunkCount(entry.chunk_count);
					header_message.SetBatchIndex(fetch_request_message.BatchIndex());
					served_start = QuackPrependHeader(*entry.payload, header_message);
					shared_ptr<MemoryStream> payload(std::move(entry.payload));
					stream.served.emplace(request.dense_index,
					                      QuackResultStream::RetainedPayload {payload, served_start, entry.rows});
					stream.retained_rows += entry.rows;
					served_batch = std::move(payload);
				}
			}
		}
		if (served_batch) {
			// outside serve_lock, because the connection's state lock guards the cache
			SyncResultCache(connection, stream, *db);
			return make_uniq<QuackRawPayloadResponse>(MessageType::FETCH_RESPONSE, std::move(served_batch),
			                                          served_start);
		}
		if (status == QuackClaimPopStatus::ERRORED) {
			return make_uniq<ErrorResponse>(stream.buffer.GetError());
		}
		if (status == QuackClaimPopStatus::FINISHED) {
			// The stream ended below this index. The terminal response carries the total, so the
			// client can check that it got every batch.
			connection.MarkQueryFinished();
			auto response = make_uniq<FetchResponseMessage>();
			if (stream.announced_total.IsValid()) {
				response->SetTotalBatches(stream.announced_total.GetIndex() - stream.prepare_batches);
			}
			return std::move(response);
		}
		if (!ArmWaker(request, stream.buffer, waker)) {
			return nullptr;
		}
	}
}

unique_ptr<QuackMessage> QuackServer::SendData(QuackRequest &request, const QuackWaker &waker) {
	auto &send_data_message = request.message->Cast<SendDataRequestMessage>();
	auto &connection = *request.connection;

	// The streams live on this connection's session, so a batch is authorized by its connection id.
	auto session_state = QuackSessionState::Get(connection.host_session->Context());
	auto stream = session_state ? session_state->Streams().Find(send_data_message.StreamId()) : nullptr;
	if (!stream) {
		return make_uniq<ErrorResponse>("No active data stream '%s'", send_data_message.StreamId());
	}
	{
		// the statement can fail before its scan sees the stream error
		annotated_lock_guard<annotated_mutex> guard(connection.statement.lock);
		if (connection.statement.stream && connection.statement.stream->buffer.HasError()) {
			return make_uniq<ErrorResponse>(connection.statement.stream->buffer.GetError());
		}
	}

	auto &incoming_chunks = send_data_message.Chunks();
	if (request.phase == QuackRequest::Phase::START && !incoming_chunks.empty()) {
		if (!send_data_message.BatchIndex().IsValid()) {
			return make_uniq<ErrorResponse>("send_data_request is missing its batch index");
		}
		idx_t batch_bytes = 0;
		for (auto &chunk : incoming_chunks) {
			batch_bytes += chunk->GetAllocationSize();
		}
		// The claim buffer drops a duplicate index, so a retry of the same batch is safe. A full buffer
		// keeps the batch with the request until the scan frees room.
		auto push = [&] {
			return stream->buffer.TryPushBatch(send_data_message.BatchIndex().GetIndex(), incoming_chunks, batch_bytes,
			                                   nullptr);
		};
		if (push() == QuackPushStatus::NO_CAPACITY) {
			ArmWaker(request, stream->buffer, waker);
			if (push() == QuackPushStatus::NO_CAPACITY) {
				return nullptr;
			}
		}
	}
	request.phase = QuackRequest::Phase::SEND;

	if (!send_data_message.TotalBatches().IsValid()) {
		if (stream->buffer.HasError()) {
			return make_uniq<ErrorResponse>(stream->buffer.GetError());
		}
		return make_uniq<SendDataResponseMessage>(); // accept_budget unset = unbounded (future flow control)
	}

	// The terminal message. Closing against a count makes a short stream fail loudly; a repeat
	// closes against the same count and gets the same answer. The statement's result, and any
	// later failure, come back through FETCH.
	stream->buffer.Finish(send_data_message.TotalBatches());
	if (stream->buffer.HasError()) {
		return make_uniq<ErrorResponse>(stream->buffer.GetError());
	}
	return make_uniq<SendDataResponseMessage>();
}

unique_ptr<QuackMessage> QuackServer::Cancel(QuackRequest &request) {
	auto &cancel_request_message = request.message->Cast<CancelRequestMessage>();
	auto &connection = *request.connection;
	// {0,0} is a wildcard — cancel whatever query is running on this connection
	bool is_wildcard = cancel_request_message.query_uuid == hugeint_t {0, 0};
	hugeint_t active_query_uuid;
	auto is_target = [&](const QuackResultStream *, hugeint_t query_uuid) {
		active_query_uuid = query_uuid;
		return is_wildcard || query_uuid == cancel_request_message.query_uuid;
	};
	// A client finds a cancel by the "Interrupt" text.
	if (!AbortStatementIf(connection, "Interrupted: query was cancelled", is_target)) {
		return make_uniq<ErrorResponse>("Attempted to cancel a different query with id '%s' instead of '%s'",
		                                cancel_request_message.query_uuid, active_query_uuid);
	}
	lock_guard<mutex> lock(connection.lock);
	connection.query_state = QuackQueryState::CANCELLED;
	connection.ClearResultCache();
	return make_uniq<SuccessResponse>();
}

unique_ptr<QuackMessage> QuackServer::Acknowledge(QuackRequest &request) {
	auto &acknowledgement_message = request.message->Cast<AcknowledgementMessage>();
	auto &connection = *request.connection;
	lock_guard<mutex> lock(connection.lock);
	auto &cache = connection.result_cache;
	if (!cache) {
		return make_uniq<SuccessResponse>(); // nothing retained, acknowledging is a no-op
	}
	if (cache->query_uuid != acknowledgement_message.QueryUUID()) {
		return make_uniq<ErrorResponse>("Attempted to acknowledge a different query with id '%s' instead of '%s'",
		                                acknowledgement_message.QueryUUID(), cache->query_uuid);
	}
	// The client confirmed it received the full result, the replay cache has served its purpose
	if (cache->stream) {
		cache->stream->DropRetention();
	}
	connection.ClearResultCache();
	return make_uniq<SuccessResponse>();
}

} // namespace duckdb

//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_fetch_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

#include "quack_claim_buffer.hpp"

namespace duckdb {

//! One sealed FETCH_RESPONSE chunk blob. The serve path writes its header, with the client-visible
//! index, directly before the blob.
struct QuackFetchPayload {
	unique_ptr<MemoryStream> payload;
	//! The end of the blob. The blob starts at QUACK_PAYLOAD_HEADER_BYTES.
	idx_t payload_size = 0;
	idx_t chunk_count = 0;
	//! The rows in the batch. The PREPARE inline drain counts them.
	idx_t rows = 0;
};
using QuackFetchPayloadBuffer = QuackClaimBuffer<QuackFetchPayload>;

//! Server state for one client-facing result. The fetch collector fills the claim buffer with sealed
//! payloads, out of order and under a byte limit. The FETCH handlers drain it.
struct QuackResultStream {
	explicit QuackResultStream(idx_t capacity_bytes) : buffer(capacity_bytes) {
	}

	QuackFetchPayloadBuffer buffer;

	void SignalBound(vector<LogicalType> types_p, vector<string> names_p) {
		{
			lock_guard<mutex> guard(bind_lock);
			types = std::move(types_p);
			names = std::move(names_p);
			bound = true;
		}
		buffer.NotifyChange();
	}

	struct Columns {
		vector<LogicalType> types;
		vector<string> names;
	};
	//! A copy of the columns: the statement may still bind while PREPARE answers
	Columns GetColumns() {
		lock_guard<mutex> guard(bind_lock);
		return Columns {types, names};
	}

	//! A scan of this statement registered a client data stream. The statement gives no row until the
	//! client sends its batches, so PREPARE answers now.
	void SignalClientDataPending() {
		{
			lock_guard<mutex> guard(bind_lock);
			client_data_pending = true;
		}
		buffer.NotifyChange();
	}

	bool Bound() {
		lock_guard<mutex> guard(bind_lock);
		return bound;
	}

	//! The statement is planned, or it waits for client data: PREPARE can answer.
	bool Answerable() {
		lock_guard<mutex> guard(bind_lock);
		return bound || client_data_pending;
	}

	//! The dense batches PREPARE consumed inline. FETCH subtracts this, so the client-facing indices
	//! stay dense from 1. PREPARE writes it before the first FETCH arrives.
	atomic<idx_t> prepare_batches {0};
	//! The dense batches the producer emitted. The terminal response carries it, so a short stream
	//! fails instead of truncating.
	optional_idx announced_total;

	//! A payload the stream keeps, header written, so a re-serve sends the same bytes again.
	struct RetainedPayload {
		shared_ptr<MemoryStream> payload;
		//! Where the wire body starts in the payload buffer.
		idx_t body_start = 0;
		idx_t rows = 0;
	};

	//! Kept until the client acks them, so a transport retry gets the SAME batch again. The client's
	//! read-ahead window bounds this map. With `retain_all` an ack releases nothing, and the map
	//! grows to the size of the result.
	mutex serve_lock;
	std::map<idx_t, RetainedPayload> served;
	//! Set while a result cache pins this stream. Guarded by serve_lock.
	bool retain_all = false;
	//! Rows in `served`. Guarded by serve_lock.
	idx_t retained_rows = 0;

	//! Keep every payload from now on. An ack then releases nothing.
	void RetainAll() {
		lock_guard<mutex> guard(serve_lock);
		retain_all = true;
	}

	//! Add a payload that the PREPARE inline drain served, outside the FETCH handler.
	void Retain(idx_t dense_index, shared_ptr<MemoryStream> payload, idx_t body_start, idx_t rows) {
		lock_guard<mutex> guard(serve_lock);
		if (!served.emplace(dense_index, RetainedPayload {std::move(payload), body_start, rows}).second) {
			return;
		}
		retained_rows += rows;
	}

	idx_t RetainedRows() {
		lock_guard<mutex> guard(serve_lock);
		return retained_rows;
	}

	//! Stop retaining and drop what is held. The result grew past the cache limit.
	void DropRetention() {
		lock_guard<mutex> guard(serve_lock);
		retain_all = false;
		served.clear();
		retained_rows = 0;
	}

private:
	mutex bind_lock;
	bool bound = false;
	bool client_data_pending = false;
	vector<LogicalType> types;
	vector<string> names;
};

//! Installed in ClientConfig::get_result_collector, so the query runs in parallel into `stream`.
unique_ptr<PhysicalOperator> MakeQuackFetchCollector(ClientContext &context, PreparedStatementData &data,
                                                     shared_ptr<QuackResultStream> stream);

} // namespace duckdb

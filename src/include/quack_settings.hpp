//===----------------------------------------------------------------------===//
//                         DuckDB
//
// quack_settings.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;

//! The defaults for the tuning settings. quack_extension.cpp and every read site use these.
static constexpr idx_t QUACK_TARGET_BATCH_BYTES_DEFAULT = 32ULL * 1024ULL * 1024ULL;
static constexpr idx_t QUACK_REBALANCE_BUFFER_BYTES_DEFAULT = 0;
static constexpr idx_t QUACK_SERVER_STREAM_BUFFER_BYTES_DEFAULT = 256ULL * 1024ULL * 1024ULL;
static constexpr idx_t QUACK_PREPARE_INLINE_ROWS_DEFAULT = 24576;
//! A statement without statement_timeout waits this long for a SereneDB server, which is the most a dead one can
//! hold it
static constexpr idx_t QUACK_DEFAULT_STATEMENT_TIMEOUT_MS_DEFAULT = 3600ULL * 1000ULL;

idx_t QuackGetUBigintSetting(ClientContext &context, const char *name, idx_t default_value);
idx_t QuackGetUBigintSetting(DatabaseInstance &db, const char *name, idx_t default_value);

//! A quack_debug_* setting, which only a debug build has
#ifdef DEBUG
inline idx_t QuackDebugSetting(ClientContext &context, const char *name) {
	return QuackGetUBigintSetting(context, name, 0);
}
#else
inline idx_t QuackDebugSetting(ClientContext &, const char *) {
	return 0;
}
#endif

//! Resolve the server budget for each FETCH or INSERT stream buffer. 0 = no byte limit.
idx_t QuackServerStreamBufferBytes(DatabaseInstance &db);

//! The checks a peer's batches get: `checks`, unless quack_check_peer_data is off. The value is the global one, so
//! the session of a server's client cannot turn them off
DeserializationChecks QuackPeerDataChecks(DatabaseInstance &db, DeserializationChecks checks);

} // namespace duckdb

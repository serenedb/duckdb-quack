#pragma once

#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

class QuackStorageExtension : public StorageExtension {
public:
	QuackStorageExtension();

	static constexpr const char *STORAGE_EXTENSION_KEY = "quack";
};

} // namespace duckdb

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

class QuackActiveConnectionsFunction {
public:
	static TableFunction GetFunction();
};

} // namespace duckdb

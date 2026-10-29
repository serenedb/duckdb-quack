# This file is included by DuckDB's build system. It specifies which extension to load

duckdb_extension_load(quack
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    INCLUDE_DIR ${CMAKE_CURRENT_LIST_DIR}/src/include
    LOAD_TESTS
    TEST_DIR ${CMAKE_CURRENT_LIST_DIR}/test
)

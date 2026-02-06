#pragma once

#include "duckdb.hpp"

namespace duckdb {
namespace cityjson {

/**
 * Register the cityjson_metadata table function
 * Returns a single row with all CityJSON metadata
 */
void RegisterMetadataTableFunction(ExtensionLoader &loader);

/**
 * Create the cityjson_metadata table function
 */
TableFunction CreateMetadataTableFunction();

} // namespace cityjson
} // namespace duckdb

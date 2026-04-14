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

/**
 * Create the cityjsonseq_metadata table function
 * Always reads metadata from a CityJSONSeq (.jsonl) file (first line)
 */
TableFunction CreateCityJSONSeqMetadataTableFunction();

/**
 * Register the cityjsonseq_metadata table function
 */
void RegisterCityJSONSeqMetadataTableFunction(ExtensionLoader &loader);

/**
 * Create the citygml_metadata table function
 * Always reads metadata from a CityGML (.gml) file
 */
TableFunction CreateCityGMLMetadataTableFunction();

/**
 * Register the citygml_metadata table function
 */
void RegisterCityGMLMetadataTableFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

#pragma once

#include "cityjson/cityjson_types.hpp"
#include "duckdb.hpp"
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * Utilities for creating and populating the metadata table
 */
class MetadataTableUtils {
public:
	/**
	 * Get the schema for the metadata table
	 */
	static vector<LogicalType> GetMetadataTableTypes();

	/**
	 * Get the column names for the metadata table
	 */
	static vector<string> GetMetadataTableNames();

	/**
	 * Create the transform_scale STRUCT type
	 */
	static LogicalType GetTransformStructType();

	/**
	 * Create the geographical_extent STRUCT type
	 */
	static LogicalType GetGeographicalExtentStructType();

	/**
	 * Create the reference_system STRUCT type
	 */
	static LogicalType GetReferenceSystemStructType();

	/**
	 * Create the point_of_contact STRUCT type
	 */
	static LogicalType GetPointOfContactStructType();

	/**
	 * Create the address STRUCT type (nested in point_of_contact)
	 */
	static LogicalType GetAddressStructType();

	/**
	 * Create a DataChunk containing metadata values
	 * @param metadata The CityJSON metadata to convert
	 * @param city_objects_count Total count of city objects
	 */
	static unique_ptr<DataChunk> CreateMetadataChunk(const CityJSON &cityjson, idx_t city_objects_count);

	/**
	 * Generate SQL CREATE TABLE statement for metadata
	 */
	static string GetCreateTableSQL(const string &table_name = "metadata");
};

} // namespace cityjson
} // namespace duckdb

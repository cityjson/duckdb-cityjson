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
	 * @param cityjson The CityJSON metadata to convert
	 * @param city_objects_count Total count of CityObjects, or an invalid
	 *        optional_idx when the source cannot report one without a full decode
	 *        (FlatCityBuf) -- rendered as SQL NULL rather than a wrong number.
	 * @param features_count Number of features (CityJSONSeq lines after the
	 *        header, FlatCityBuf features), or invalid for a whole-document
	 *        CityJSON, which has no feature concept.
	 */
	static unique_ptr<DataChunk> CreateMetadataChunk(const CityJSON &cityjson, optional_idx city_objects_count,
	                                                 optional_idx features_count);
};

} // namespace cityjson
} // namespace duckdb

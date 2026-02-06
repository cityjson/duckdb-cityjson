#pragma once

#include "cityjson/types.hpp"
#include "cityjson/cityjson_types.hpp"
#include <string>
#include <vector>
#include <set>

namespace duckdb {
namespace cityjson {

/**
 * Definition for a per-LOD table in the new geometry encoding scheme
 *
 * Each LOD level gets its own table with:
 * - Standard columns (id, feature_id, object_type, attributes)
 * - Single geometry column (WKB BLOB)
 * - Single geometry_properties column (JSON)
 */
struct LODTableDefinition {
	std::string table_name;      // e.g., "buildings_lod2"
	std::string lod_value;       // e.g., "2.2"
	std::vector<Column> columns; // Full column list

	LODTableDefinition() = default;
	LODTableDefinition(std::string name, std::string lod) : table_name(std::move(name)), lod_value(std::move(lod)) {
	}
};

/**
 * Utility class for LOD table schema operations
 */
class LODTableUtils {
public:
	/**
	 * Generate table name for a given LOD
	 * Converts LOD "2.2" to table name "buildings_lod2_2"
	 *
	 * @param lod LOD string (e.g., "2.2")
	 * @param base_name Optional base name (default: "geometry")
	 * @return Table name (e.g., "geometry_lod2_2")
	 */
	static std::string GetTableNameForLOD(const std::string &lod, const std::string &base_name = "geometry");

	/**
	 * Get base columns that appear in every LOD table
	 * These are the standard columns without geometry
	 *
	 * Columns:
	 * - id: VARCHAR
	 * - feature_id: VARCHAR
	 * - object_type: VARCHAR
	 * - children: LIST(VARCHAR)
	 * - parents: LIST(VARCHAR)
	 *
	 * @return Vector of base column definitions
	 */
	static std::vector<Column> GetBaseColumns();

	/**
	 * Get geometry-specific columns for WKB encoding
	 *
	 * Columns:
	 * - geometry: BLOB (WKB)
	 * - geometry_properties: JSON
	 *
	 * @return Vector of geometry column definitions
	 */
	static std::vector<Column> GetGeometryColumns();

	/**
	 * Collect all unique LOD values from features
	 *
	 * @param features Vector of CityJSONFeature to scan
	 * @param sample_size Maximum number of features to sample
	 * @return Set of unique LOD strings (sorted)
	 */
	static std::set<std::string> CollectLODs(const std::vector<CityJSONFeature> &features, size_t sample_size = 100);

	/**
	 * Infer per-LOD table definitions from features
	 *
	 * For each unique LOD found in the features, creates a table definition with:
	 * - Base columns (id, feature_id, object_type, etc.)
	 * - Inferred attribute columns
	 * - Geometry columns (WKB + properties)
	 *
	 * @param features Vector of CityJSONFeature to sample from
	 * @param sample_size Maximum number of features to sample
	 * @return Vector of LOD table definitions (one per unique LOD)
	 */
	static std::vector<LODTableDefinition> InferLODTables(const std::vector<CityJSONFeature> &features,
	                                                      size_t sample_size = 100);

	/**
	 * Parse LOD from column name format to LOD value format
	 * Example: "lod2_2" -> "2.2"
	 *
	 * @param column_suffix LOD suffix from column name
	 * @return LOD value string
	 */
	static std::string ParseLODFromSuffix(const std::string &column_suffix);

	/**
	 * Format LOD value as column name suffix
	 * Example: "2.2" -> "lod2_2"
	 *
	 * @param lod LOD value string
	 * @return Column name suffix
	 */
	static std::string FormatLODAsColumnSuffix(const std::string &lod);
};

} // namespace cityjson
} // namespace duckdb

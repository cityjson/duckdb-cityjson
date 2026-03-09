#pragma once

#include "cityjson/types.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb.hpp"
#include <vector>
#include <string>

namespace duckdb {
namespace cityjson {

/**
 * Utility class for column type operations
 * Provides conversion between ColumnType enum and DuckDB types
 */
class ColumnTypeUtils {
public:
	/**
	 * Convert ColumnType to SQL type string representation
	 * Used for DDL generation and debugging
	 *
	 * @param type Column type enum
	 * @return SQL type string (e.g., "BIGINT", "VARCHAR", "STRUCT(...)")
	 */
	static const char *ToString(ColumnType type);

	/**
	 * Convert ColumnType to DuckDB LogicalTypeId
	 * Used for simple types that map 1:1 to DuckDB types
	 *
	 * @param type Column type enum
	 * @return DuckDB logical type ID
	 */
	static LogicalTypeId ToLogicalTypeId(ColumnType type);

	/**
	 * Convert ColumnType to full DuckDB LogicalType
	 * Handles complex types (LIST, STRUCT) with complete type information
	 *
	 * @param type Column type enum
	 * @return DuckDB LogicalType (may be complex type with children)
	 */
	static LogicalType ToDuckDBType(ColumnType type);

	/**
	 * Parse column type from string (case-insensitive)
	 * Supports type aliases:
	 * - INT/INTEGER/BIGINT → BigInt
	 * - FLOAT/DOUBLE → Double
	 * - TEXT/STRING/VARCHAR → Varchar
	 * - BOOL/BOOLEAN → Boolean
	 *
	 * @param name Type name string
	 * @return Parsed column type
	 * @throws CityJSONError if type name is invalid
	 */
	static ColumnType Parse(const std::string &name);

	/**
	 * Infer column type from JSON value
	 * Used during schema inference from sample data
	 *
	 * Inference rules:
	 * - null → Varchar (fallback type)
	 * - boolean → Boolean
	 * - integer → BigInt
	 * - floating point → Double
	 * - string → Varchar (or Timestamp/Date/Time if format matches)
	 * - array → Json (or VarcharArray if all elements are strings)
	 * - object → Json
	 *
	 * @param value JSON value to analyze
	 * @return Inferred column type
	 */
	static ColumnType InferFromJson(const json &value);

	/**
	 * Resolve final column type from multiple sampled values
	 * Handles type promotion and inconsistency across samples
	 *
	 * Resolution rules:
	 * - All same type → that type
	 * - Mixed BigInt/Double → Double (numeric promotion)
	 * - Any other mix → Varchar (fallback for inconsistency)
	 * - Empty samples → Varchar (default)
	 *
	 * @param types Vector of observed types from samples
	 * @return Resolved column type
	 */
	static ColumnType ResolveFromSamples(const std::vector<ColumnType> &types);

	/**
	 * Check if type is numeric (BigInt or Double)
	 *
	 * @param type Column type to check
	 * @return true if type is BigInt or Double
	 */
	static bool IsNumeric(ColumnType type);

	/**
	 * Check if type is temporal (Timestamp, Date, or Time)
	 *
	 * @param type Column type to check
	 * @return true if type is temporal
	 */
	static bool IsTemporal(ColumnType type);

	/**
	 * Check if type is complex (needs special handling)
	 *
	 * @param type Column type to check
	 * @return true if type is Geometry, GeographicalExtent, VarcharArray, or Json
	 */
	static bool IsComplex(ColumnType type);

	/**
	 * Try to parse string as date/time and return appropriate type
	 * Returns nullopt if string doesn't match any temporal format
	 *
	 * @param str String value to check
	 * @return Optional temporal type if pattern matches
	 */
	static std::optional<ColumnType> InferTemporalType(const std::string &str);
};

/**
 * Get predefined columns for CityJSON city objects
 * These columns are always present in the output schema
 *
 * Standard columns (in order):
 * 1. id: VARCHAR - CityObject ID (unique within feature)
 * 2. feature_id: VARCHAR - Feature ID (groups CityObjects into CityJSONFeatures)
 * 3. object_type: VARCHAR - CityObject type (Building, Road, Bridge, etc.)
 * 4. children: LIST(VARCHAR) - Child CityObject IDs
 * 5. children_roles: LIST(VARCHAR) - Roles of child CityObjects
 * 6. parents: LIST(VARCHAR) - Parent CityObject IDs
 * 7. other: JSON - Custom/extension fields not in standard attributes
 *
 * Optional (not currently included, reserved for future):
 * - geographical_extent: STRUCT - 3D bounding box
 * - geom_lod{X}_{Y}: STRUCT - Geometry at specific LOD
 *
 * @return Vector of predefined Column definitions
 */
std::vector<Column> GetDefinedColumns();

/**
 * Check if column name is a predefined column
 *
 * @param name Column name to check
 * @return true if name is a predefined column
 */
bool IsPredefinedColumn(const std::string &name);

/**
 * Check if column name is a geometry column (pattern: geom_lod{X}_{Y})
 *
 * @param name Column name to check
 * @return true if name matches geometry column pattern
 */
bool IsGeometryColumn(const std::string &name);

/**
 * Parse LOD from geometry column name
 * Example: "geom_lod2_1" → "2.1"
 *
 * @param column_name Geometry column name
 * @return LOD string (e.g., "2.1")
 * @throws CityJSONError if column name is invalid
 */
std::string ParseLODFromColumnName(const std::string &column_name);

} // namespace cityjson
} // namespace duckdb

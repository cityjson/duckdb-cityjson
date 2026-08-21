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
 * The **leading (head) run** of the spec's reserved column order
 * (02-object-table-schema.mdx, "Reserved columns") -- everything up to but not
 * including `bbox`:
 *
 * 1. id: VARCHAR
 * 2. feature_id: VARCHAR
 * 3. object_type: VARCHAR
 * 4. parents: LIST(VARCHAR)
 * 5. children: LIST(VARCHAR)
 * 6. children_roles: LIST(VARCHAR)
 * 7. address: LIST(STRUCT) -- always NULL until a reader parses source addresses
 *
 * A caller assembles the full reserved order by inserting the geometry columns
 * after this, then `LODTableUtils::GetTrailingColumns()` (`template`, `other`),
 * before any attribute column.
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
 * Check if a name collides (case-insensitively) with any reserved output column.
 *
 * Reserved columns are the predefined structural columns plus the wide-layout
 * geometry columns (`geometry`, `geometry_lod*`, `geometry_properties_lod*`) and
 * `bbox`. Reserved columns take precedence over dynamic attributes: a source
 * attribute whose name collides with a reserved column must not be emitted as its
 * own column (it is preserved in the `other` JSON instead), otherwise DuckDB would
 * see duplicate column names (it is case-insensitive) and refuse to bind.
 *
 * @param name Column/attribute name to check
 * @return true if the name case-insensitively matches a reserved column
 */
bool IsReservedColumnName(const std::string &name);

/**
 * Check if a column name is a per-LoD appearance column (§11): exactly
 * `material` / `texture`, or `material_lod{X}` / `texture_lod{X}` /
 * `..._lod{X}_{Y}`. Matches the reserved suffix grammar precisely so an
 * attribute like `material_lodging` is NOT misclassified.
 *
 * @param name Column name to check
 * @return true if name is an appearance column
 */
bool IsAppearanceColumnName(const std::string &name);

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

/**
 * Parse LOD from a default-mode WKB geometry column name.
 * Handles both "geometry_lod2_2" and "geometry_properties_lod2_2" → "2.2",
 * and single-component LODs such as "geometry_lod0" → "0".
 *
 * @param column_name WKB geometry or geometry-properties column name
 * @return Normalised LOD string (e.g., "2.2")
 * @throws CityJSONError if no LOD component is found
 */
std::string ParseLODFromGeometryColumn(const std::string &column_name);

} // namespace cityjson
} // namespace duckdb

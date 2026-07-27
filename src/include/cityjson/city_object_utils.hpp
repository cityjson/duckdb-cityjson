#pragma once

#include "cityjson/types.hpp"
#include "cityjson/cityjson_types.hpp"
#include "cityjson/json_utils.hpp"
#include <vector>
#include <string>

namespace duckdb {
namespace cityjson {

/**
 * Utility class for CityObject attribute extraction and schema inference
 * Provides static methods for working with CityObject attributes and geometries
 */
class CityObjectUtils {
public:
	/**
	 * Get attribute value from CityObject for a specific column
	 * Handles both predefined columns and dynamic attribute columns
	 *
	 * Predefined columns handled:
	 * - "object_type" → CityObject.type
	 * - "children" → CityObject.children
	 * - "parents" → CityObject.parents
	 * - "children_roles" → CityObject.children_roles
	 * - "geographical_extent" → CityObject.geographical_extent
	 * - "other" → Attributes not in predefined/dynamic columns
	 *
	 * Dynamic columns:
	 * - Looks up attribute by column name in CityObject.attributes
	 * - Returns null JSON if attribute doesn't exist
	 *
	 * @param obj CityObject to extract from
	 * @param col Column definition
	 * @return JSON value for the column (may be null)
	 */
	static json GetAttributeValue(const CityObject &obj, const Column &col);

	/**
	 * Get geometry value from CityObject for a geometry column
	 * Geometry column name format: "geom_lod{X}_{Y}" → LOD "X.Y"
	 *
	 * Example:
	 * - "geom_lod2_1" → looks for geometry with LOD "2.1"
	 * - "geom_lod1_0" → looks for geometry with LOD "1.0"
	 *
	 * @param obj CityObject to extract from
	 * @param col Geometry column definition
	 * @return JSON representation of geometry (or null if not found)
	 * @throws CityJSONError::InvalidSchema if column name is invalid
	 */
	static json GetGeometryValue(const CityObject &obj, const Column &col);

	/**
	 * Infer attribute columns from sample features
	 * Scans CityObjects in features to discover all attribute keys and infer types
	 *
	 * Algorithm:
	 * 1. Sample up to N features (or all if fewer available)
	 * 2. Collect all attribute keys from CityObjects in sampled features
	 * 3. For each attribute key:
	 *    a. Collect all observed values across samples
	 *    b. Infer type for each value using ColumnTypeUtils::InferFromJson()
	 *    c. Resolve final type using ColumnTypeUtils::ResolveFromSamples()
	 * 4. Exclude predefined column names from results
	 * 5. Return sorted list of inferred columns
	 *
	 * @param features Vector of CityJSONFeature to sample from
	 * @param sample_size Maximum number of features to sample (default: 100)
	 * @return Vector of inferred Column definitions (sorted by name)
	 */
	static std::vector<Column> InferAttributeColumns(const std::vector<CityJSONFeature> &features,
	                                                 size_t sample_size = 100);

	/**
	 * Infer geometry columns from sample features
	 * Scans geometries to discover all LODs present
	 *
	 * Algorithm:
	 * 1. Sample up to N features (or all if fewer available)
	 * 2. Collect all unique LODs from geometries in sampled CityObjects
	 * 3. For each LOD, create geometry column: "geom_lod{X}_{Y}"
	 * 4. Return sorted list of geometry columns
	 *
	 * @param features Vector of CityJSONFeature to sample from
	 * @param sample_size Maximum number of features to sample (default: 100)
	 * @return Vector of geometry Column definitions (sorted by LOD)
	 */
	static std::vector<Column> InferGeometryColumns(const std::vector<CityJSONFeature> &features,
	                                                size_t sample_size = 100);

	/**
	 * Encode geometry to WKB format
	 *
	 * @param geometry Geometry object to encode
	 * @param vertices Shared vertex array from CityJSON
	 * @param transform Optional transform to apply to vertices
	 * @return WKB binary data as vector of bytes
	 */
	static std::vector<uint8_t> GetGeometryWKB(const Geometry &geometry,
	                                           const std::vector<std::array<double, 3>> &vertices,
	                                           const std::optional<Transform> &transform);

	/**
	 * Compute the 3D bounding box of a geometry from its boundary vertices.
	 *
	 * Walks the geometry boundaries, resolves each vertex index in the pool,
	 * applies the transform, and returns the min/max extent. Returns nullopt
	 * if the geometry references no valid vertices.
	 */
	/**
	 * The extent stored in a city object's `bbox` column: the union of the object's own
	 * geometry across **every** stored LoD *and* across all of its descendants, per the
	 * CityParquet specification.
	 *
	 * Both halves matter. Taking only the highest LoD understates an object whose LoDs
	 * differ in extent; ignoring descendants understates the common case where a
	 * Building carries only an LoD0 footprint while its BuildingPart carries the solid,
	 * which would make the parent's bbox useless for spatial pruning — a query filtering
	 * on it would silently miss the building.
	 *
	 * `objects` is the pool the object's `children` ids resolve against (one
	 * CityJSONFeature's objects, or the whole document's for plain CityJSON). Cycles in
	 * the hierarchy are tolerated: each id is visited once.
	 */
	static std::optional<GeographicalExtent>
	GetObjectExtent(const std::string &object_id, const std::map<std::string, CityObject> &objects,
	                const std::vector<std::array<double, 3>> &vertices,
	                const std::optional<Transform> &transform);

	static std::optional<GeographicalExtent>
	GetGeometryExtent(const Geometry &geometry, const std::vector<std::array<double, 3>> &vertices,
	                  const std::optional<Transform> &transform);

	/**
	 * Serialize geometry properties to the spec §8 payload
	 *
	 * @param geometry Geometry object to serialize
	 * @param object_id Optional parent CityObject ID
	 * @return JSON object with geometry properties (type, surfaces, face_semantics, shells)
	 */
	static json GetGeometryPropertiesStruct(const Geometry &geometry,
	                                        const std::optional<std::string> &object_id = std::nullopt);
};

} // namespace cityjson
} // namespace duckdb

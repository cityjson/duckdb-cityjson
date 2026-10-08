#pragma once

#include "cityjson/types.hpp"
#include "cityjson/cityjson_types.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb/common/types/value.hpp"
#include <set>
#include <vector>
#include <string>

namespace duckdb {
class ClientContext;

namespace cityjson {

/**
 * Report, as a DuckDB warning, every geometry of `features` that `Geometry::FromJson`
 * normalised: rings of fewer than three vertices it dropped, and the surfaces whose
 * exterior ring that was (spec 03-geometry-semantics.mdx: a writer that normalises a
 * ring away SHOULD report what it removed). One warning per affected geometry.
 */
void ReportDroppedRings(ClientContext &context, const std::vector<CityJSONFeature> &features);
void ReportDroppedRings(ClientContext &context, const CityJSONFeature &feature);

/**
 * Utility class for CityObject attribute extraction and schema inference
 * Provides static methods for working with CityObject attributes and geometries
 */
class CityObjectUtils {
public:
	/**
	 * Get attribute value from CityObject for a specific column.
	 *
	 * `emitted_columns` names every column the bound schema actually
	 * produces. `other` is assembled from the members that have none, so it
	 * needs the real column set rather than a static predicate: an attribute
	 * that lost a case-insensitive dedup race has no column of its own and
	 * must still survive in `other`.
	 */
	static json GetAttributeValue(const CityObject &obj, const Column &col,
	                              const std::set<std::string> &emitted_columns);

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
	static std::optional<GeographicalExtent> GetObjectExtent(const std::string &object_id,
	                                                         const std::map<std::string, CityObject> &objects,
	                                                         const std::vector<std::array<double, 3>> &vertices,
	                                                         const std::optional<Transform> &transform);

	static std::optional<GeographicalExtent> GetGeometryExtent(const Geometry &geometry,
	                                                           const std::vector<std::array<double, 3>> &vertices,
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

	/**
	 * The object's `address` column value (spec 02-object-table-schema.mdx,
	 * "Addresses"): one struct per source address, each recognised member in its field
	 * and `location` -- a MultiPoint indexing `vertices` -- as WKB MultiPointZ. NULL when
	 * the object has no address. See AddressMemberNames for the member vocabulary.
	 */
	static Value GetAddressValue(const CityObject &object, const std::vector<std::array<double, 3>> *vertices,
	                             const std::optional<Transform> &transform);
};

/**
 * The CityJSON address member each `address` struct field is read from and written
 * back to, in struct field order (street, house_number, po_box, zip_code, city, state,
 * country, free_text). CityJSON prescribes no member names; `thoroughfareName`,
 * `thoroughfareNumber`, `postcode`, `locality` and `country` are CityJSON 2.0.1's own
 * documented example, `postBox`, `administrativeArea` and `freeText` name the three
 * fields it has no example for. A member spelt otherwise is not retained.
 */
const std::vector<std::pair<std::string, std::string>> &AddressMemberNames();

} // namespace cityjson
} // namespace duckdb

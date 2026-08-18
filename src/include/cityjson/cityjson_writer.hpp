#pragma once

#include "cityjson/cityjson_types.hpp"
#include "cityjson/json_utils.hpp"
#include <string>
#include <vector>
#include <map>
#include <array>
#include <optional>

namespace duckdb {
namespace cityjson {

/**
 * Consolidated metadata for CityJSON write operations
 */
struct CityJSONWriteMetadata {
	std::string version = "2.0";
	std::optional<std::string> crs;
	std::optional<Transform> transform;
	std::optional<std::string> title;
	std::optional<std::string> identifier;
	std::optional<std::string> reference_date;
	std::optional<GeographicalExtent> geographical_extent;
	std::optional<PointOfContact> point_of_contact;
};

/**
 * Utilities for writing CityJSON and CityJSONSeq files
 */
class CityJSONWriter {
public:
	/**
	 * Write a complete CityJSON file (.city.json)
	 *
	 * @param file_path Output path
	 * @param metadata Write metadata (version, CRS, transform, title, etc.)
	 * @param feature_objects Map of feature_id -> [(city_object_id, city_object_json)]
	 * @param feature_order Ordered feature IDs
	 */
	static void WriteCityJSON(const std::string &file_path, const CityJSONWriteMetadata &metadata,
	                          const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
	                          const std::vector<std::string> &feature_order,
	                          const std::optional<json> &appearance = std::nullopt);

	/**
	 * Write a CityJSONSeq file (.city.jsonl)
	 * Line 1: metadata header
	 * Line 2+: one CityJSONFeature per line with per-feature vertex pool
	 *
	 * @param file_path Output path
	 * @param metadata Write metadata (version, CRS, transform, title, etc.)
	 * @param feature_objects Map of feature_id -> [(city_object_id, city_object_json)]
	 * @param feature_order Ordered feature IDs
	 */
	static void
	WriteCityJSONSeq(const std::string &file_path, const CityJSONWriteMetadata &metadata,
	                 const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
	                 const std::vector<std::string> &feature_order,
	                 const std::optional<json> &appearance_header = std::nullopt,
	                 const std::map<std::string, json> &appearance_by_feature = {});

#ifdef CITYJSON_HAS_FCB
	/**
	 * Write a FlatCityBuf file (.fcb)
	 * Uses the FCB writer API to produce a cloud-optimized binary format.
	 * Internally builds CityJSONSeq-style JSON (metadata header + per-feature JSON)
	 * and feeds it to the FCB writer.
	 *
	 * @param file_path Output path
	 * @param metadata Write metadata (version, CRS, transform, title, etc.)
	 * @param feature_objects Map of feature_id -> [(city_object_id, city_object_json)]
	 * @param feature_order Ordered feature IDs
	 * @param attr_index_columns Attribute column names to give a B+tree index
	 * @param branching_factor B+tree branching factor applied to every indexed column
	 * @param index_node_size R-tree node size
	 * @param declared_attr_columns Every attribute column of the source relation.
	 *        The header schema is otherwise derived from observed JSON values via
	 *        fcb::add_attributes, whose guess_type cannot type a null -- and the
	 *        COPY sink omits null attributes from the JSON entirely, so a column
	 *        that is null in every object is invisible to that derivation and
	 *        vanishes from the file. The relation's column list is the authority
	 *        on which attributes exist; this restores the six all-null columns a
	 *        74-column 3DBAG source otherwise loses on the way into FCB.
	 */
	static void WriteFlatCityBuf(const std::string &file_path, const CityJSONWriteMetadata &metadata,
	                             std::map<std::string, std::vector<std::pair<std::string, json>>> feature_objects,
	                             const std::vector<std::string> &feature_order,
	                             const std::vector<std::string> &attr_index_columns = {},
	                             std::optional<uint16_t> branching_factor = std::nullopt,
	                             std::optional<uint16_t> index_node_size = std::nullopt,
	                             const std::vector<std::string> &declared_attr_columns = {});
#endif

private:
	/**
	 * Build the metadata JSON object from write metadata
	 */
	static json BuildMetadataJson(const CityJSONWriteMetadata &metadata);

	/**
	 * Build a vertex pool from geometry boundaries, replacing coordinates with indices
	 * Returns the vertex pool and modifies the CityObject JSON in-place
	 */
	static std::vector<std::array<int64_t, 3>> BuildVertexPool(std::vector<std::pair<std::string, json>> &objects,
	                                                           const std::optional<Transform> &transform);
};

} // namespace cityjson
} // namespace duckdb

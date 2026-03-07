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
	static void WriteCityJSON(
	    const std::string &file_path,
	    const CityJSONWriteMetadata &metadata,
	    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
	    const std::vector<std::string> &feature_order);

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
	static void WriteCityJSONSeq(
	    const std::string &file_path,
	    const CityJSONWriteMetadata &metadata,
	    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
	    const std::vector<std::string> &feature_order);

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
	 */
	static void WriteFlatCityBuf(
	    const std::string &file_path,
	    const CityJSONWriteMetadata &metadata,
	    std::map<std::string, std::vector<std::pair<std::string, json>>> feature_objects,
	    const std::vector<std::string> &feature_order);
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
	static std::vector<std::array<int64_t, 3>> BuildVertexPool(
	    std::vector<std::pair<std::string, json>> &objects,
	    const std::optional<Transform> &transform);
};

} // namespace cityjson
} // namespace duckdb

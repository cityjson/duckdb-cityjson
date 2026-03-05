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
 * Utilities for writing CityJSON and CityJSONSeq files
 */
class CityJSONWriter {
public:
	/**
	 * Write a complete CityJSON file (.city.json)
	 *
	 * @param file_path Output path
	 * @param version CityJSON version string
	 * @param crs Optional coordinate reference system
	 * @param transform Optional transform (scale/translate) for vertex quantisation
	 * @param feature_objects Map of feature_id -> [(city_object_id, city_object_json)]
	 * @param feature_order Ordered feature IDs
	 */
	static void WriteCityJSON(
	    const std::string &file_path,
	    const std::string &version,
	    const std::optional<std::string> &crs,
	    const std::optional<Transform> &transform,
	    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
	    const std::vector<std::string> &feature_order);

	/**
	 * Write a CityJSONSeq file (.city.jsonl)
	 * Line 1: metadata header
	 * Line 2+: one CityJSONFeature per line with per-feature vertex pool
	 *
	 * @param file_path Output path
	 * @param version CityJSON version string
	 * @param crs Optional coordinate reference system
	 * @param transform Optional transform (scale/translate) for vertex quantisation
	 * @param feature_objects Map of feature_id -> [(city_object_id, city_object_json)]
	 * @param feature_order Ordered feature IDs
	 */
	static void WriteCityJSONSeq(
	    const std::string &file_path,
	    const std::string &version,
	    const std::optional<std::string> &crs,
	    const std::optional<Transform> &transform,
	    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
	    const std::vector<std::string> &feature_order);

private:
	/**
	 * Quantise a real coordinate to an integer index
	 */
	static std::array<int64_t, 3> QuantiseVertex(
	    const std::array<double, 3> &coord,
	    const Transform &transform);

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

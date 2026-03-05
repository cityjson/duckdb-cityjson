#include "cityjson/cityjson_writer.hpp"
#include <fstream>
#include <cmath>
#include <map>

namespace duckdb {
namespace cityjson {

// json is already a typedef in duckdb::cityjson namespace via json_utils.hpp

// ============================================================
// QuantiseVertex (file-local helper)
// ============================================================

static std::array<int64_t, 3> QuantiseVertex(
    const std::array<double, 3> &coord,
    const Transform &transform) {
	return {
	    static_cast<int64_t>(std::round((coord[0] - transform.translate[0]) / transform.scale[0])),
	    static_cast<int64_t>(std::round((coord[1] - transform.translate[1]) / transform.scale[1])),
	    static_cast<int64_t>(std::round((coord[2] - transform.translate[2]) / transform.scale[2]))
	};
}

// ============================================================
// CollectAndReplaceVertices - recursive helper
// ============================================================

// Recursively walk boundaries and collect/replace vertex coordinates with indices
static void CollectAndReplaceVertices(
    json &boundaries,
    std::map<std::tuple<int64_t, int64_t, int64_t>, size_t> &vertex_map,
    std::vector<std::array<int64_t, 3>> &vertex_pool,
    const std::optional<Transform> &transform,
    int depth) {
	if (!boundaries.is_array()) {
		return;
	}

	// At depth 0, each element is a vertex index (integer) or coordinate array
	if (depth == 0) {
		for (auto &elem : boundaries) {
			if (elem.is_number_integer()) {
				// Already an index - leave as is (shouldn't happen in our generated JSON)
				continue;
			}
			if (elem.is_array() && elem.size() == 3) {
				// This is a coordinate [x, y, z]
				std::array<double, 3> coord = {
				    elem[0].get<double>(),
				    elem[1].get<double>(),
				    elem[2].get<double>()
				};

				std::array<int64_t, 3> quantised;
				if (transform.has_value()) {
					quantised = QuantiseVertex(coord, transform.value());
				} else {
					// No transform: store as-is (rounded to int)
					quantised = {
					    static_cast<int64_t>(std::round(coord[0])),
					    static_cast<int64_t>(std::round(coord[1])),
					    static_cast<int64_t>(std::round(coord[2]))
					};
				}

				auto key = std::make_tuple(quantised[0], quantised[1], quantised[2]);
				auto it = vertex_map.find(key);
				size_t idx;
				if (it != vertex_map.end()) {
					idx = it->second;
				} else {
					idx = vertex_pool.size();
					vertex_pool.push_back(quantised);
					vertex_map[key] = idx;
				}

				// Replace the coordinate array with the index
				elem = static_cast<int64_t>(idx);
			}
		}
	} else {
		for (auto &child : boundaries) {
			CollectAndReplaceVertices(child, vertex_map, vertex_pool, transform, depth - 1);
		}
	}
}

// Determine nesting depth of geometry boundaries based on geometry type
static int GetBoundaryDepth(const std::string &geom_type) {
	if (geom_type == "MultiPoint") return 1;
	if (geom_type == "MultiLineString") return 2;
	if (geom_type == "MultiSurface" || geom_type == "CompositeSurface") return 3;
	if (geom_type == "Solid") return 4;
	if (geom_type == "MultiSolid" || geom_type == "CompositeSolid") return 5;
	return 3; // default
}

// ============================================================
// BuildVertexPool
// ============================================================

std::vector<std::array<int64_t, 3>> CityJSONWriter::BuildVertexPool(
    std::vector<std::pair<std::string, json>> &objects,
    const std::optional<Transform> &transform) {

	std::map<std::tuple<int64_t, int64_t, int64_t>, size_t> vertex_map;
	std::vector<std::array<int64_t, 3>> vertex_pool;

	for (auto &[obj_id, obj_json] : objects) {
		if (!obj_json.contains("geometry") || !obj_json["geometry"].is_array()) {
			continue;
		}

		for (auto &geom : obj_json["geometry"]) {
			if (!geom.contains("boundaries") || !geom["boundaries"].is_array()) {
				continue;
			}

			std::string geom_type = geom.value("type", "MultiSurface");
			int depth = GetBoundaryDepth(geom_type);
			CollectAndReplaceVertices(geom["boundaries"], vertex_map, vertex_pool, transform, depth);
		}
	}

	return vertex_pool;
}

// ============================================================
// WriteCityJSON
// ============================================================

void CityJSONWriter::WriteCityJSON(
    const std::string &file_path,
    const std::string &version,
    const std::optional<std::string> &crs,
    const std::optional<Transform> &transform,
    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
    const std::vector<std::string> &feature_order) {

	// Build the root CityJSON object
	json root;
	root["type"] = "CityJSON";
	root["version"] = version;

	// Metadata
	if (crs.has_value()) {
		root["metadata"] = json::object();
		root["metadata"]["referenceSystem"] = crs.value();
	}

	// Transform
	if (transform.has_value()) {
		root["transform"] = json::object();
		root["transform"]["scale"] = json::array({
		    transform->scale[0], transform->scale[1], transform->scale[2]
		});
		root["transform"]["translate"] = json::array({
		    transform->translate[0], transform->translate[1], transform->translate[2]
		});
	}

	// Collect all city objects for vertex pool building
	std::vector<std::pair<std::string, json>> all_objects;
	for (const auto &fid : feature_order) {
		auto it = feature_objects.find(fid);
		if (it == feature_objects.end()) continue;
		for (const auto &[obj_id, obj_json] : it->second) {
			all_objects.emplace_back(obj_id, obj_json);
		}
	}

	// Build global vertex pool (replaces coordinates with indices in-place)
	auto vertex_pool = BuildVertexPool(all_objects, transform);

	// CityObjects
	root["CityObjects"] = json::object();
	for (const auto &[obj_id, obj_json] : all_objects) {
		root["CityObjects"][obj_id] = obj_json;
	}

	// Vertices
	root["vertices"] = json::array();
	for (const auto &v : vertex_pool) {
		root["vertices"].push_back(json::array({v[0], v[1], v[2]}));
	}

	// Write to file
	std::ofstream out(file_path);
	if (!out.is_open()) {
		throw CityJSONError::FileRead("Failed to open output file: " + file_path);
	}
	out << root.dump();
}

// ============================================================
// WriteCityJSONSeq
// ============================================================

void CityJSONWriter::WriteCityJSONSeq(
    const std::string &file_path,
    const std::string &version,
    const std::optional<std::string> &crs,
    const std::optional<Transform> &transform,
    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
    const std::vector<std::string> &feature_order) {

	std::ofstream out(file_path);
	if (!out.is_open()) {
		throw CityJSONError::FileRead("Failed to open output file: " + file_path);
	}

	// Line 1: metadata header
	json header;
	header["type"] = "CityJSON";
	header["version"] = version;
	header["CityObjects"] = json::object();
	header["vertices"] = json::array();

	if (crs.has_value()) {
		header["metadata"] = json::object();
		header["metadata"]["referenceSystem"] = crs.value();
	}

	if (transform.has_value()) {
		header["transform"] = json::object();
		header["transform"]["scale"] = json::array({
		    transform->scale[0], transform->scale[1], transform->scale[2]
		});
		header["transform"]["translate"] = json::array({
		    transform->translate[0], transform->translate[1], transform->translate[2]
		});
	}

	out << header.dump() << "\n";

	// Line 2+: one CityJSONFeature per feature_id, with per-feature vertex pool
	for (const auto &fid : feature_order) {
		auto it = feature_objects.find(fid);
		if (it == feature_objects.end()) continue;

		// Copy objects for this feature (we'll modify them for vertex pool building)
		auto feature_objs = it->second;

		// Build per-feature vertex pool
		auto vertex_pool = BuildVertexPool(feature_objs, transform);

		// Build CityJSONFeature line
		json feature;
		feature["type"] = "CityJSONFeature";
		feature["id"] = fid;
		feature["CityObjects"] = json::object();
		for (const auto &[obj_id, obj_json] : feature_objs) {
			feature["CityObjects"][obj_id] = obj_json;
		}

		feature["vertices"] = json::array();
		for (const auto &v : vertex_pool) {
			feature["vertices"].push_back(json::array({v[0], v[1], v[2]}));
		}

		out << feature.dump() << "\n";
	}
}

} // namespace cityjson
} // namespace duckdb

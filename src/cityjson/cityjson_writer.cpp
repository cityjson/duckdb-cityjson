#include "cityjson/cityjson_writer.hpp"
#ifdef CITYJSON_HAS_FCB
#include "fcb.h"
#endif
#include <fstream>
#include <cmath>
#include <map>

namespace duckdb {
namespace cityjson {

// json is already a typedef in duckdb::cityjson namespace via json_utils.hpp

// ============================================================
// QuantiseVertex (file-local helper)
// ============================================================

static std::array<int64_t, 3> QuantiseVertex(const std::array<double, 3> &coord, const Transform &transform) {
	return {static_cast<int64_t>(std::round((coord[0] - transform.translate[0]) / transform.scale[0])),
	        static_cast<int64_t>(std::round((coord[1] - transform.translate[1]) / transform.scale[1])),
	        static_cast<int64_t>(std::round((coord[2] - transform.translate[2]) / transform.scale[2]))};
}

// ============================================================
// CollectAndReplaceVertices - recursive helper
// ============================================================

// Recursively walk boundaries and collect/replace vertex coordinates with indices
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
static void CollectAndReplaceVertices(json &boundaries,
                                      std::map<std::tuple<int64_t, int64_t, int64_t>, size_t> &vertex_map,
                                      std::vector<std::array<int64_t, 3>> &vertex_pool,
                                      const std::optional<Transform> &transform, int depth) {
	if (!boundaries.is_array()) {
		return;
	}

	// At depth 0, each element is a vertex index (integer) or coordinate array [x,y,z]
	if (depth == 0) {
		for (auto &elem : boundaries) {
			if (elem.is_number_integer()) {
				// Already an index - leave as is
				continue;
			}
			if (elem.is_array() && elem.size() == 3 && elem[0].is_number() && elem[1].is_number() &&
			    elem[2].is_number()) {
				// This is a coordinate [x, y, z]
				std::array<double, 3> coord = {elem[0].get<double>(), elem[1].get<double>(), elem[2].get<double>()};

				std::array<int64_t, 3> quantised;
				if (transform.has_value()) {
					quantised = QuantiseVertex(coord, transform.value());
				} else {
					// No transform: store as-is (rounded to int)
					quantised = {static_cast<int64_t>(std::round(coord[0])), static_cast<int64_t>(std::round(coord[1])),
					             static_cast<int64_t>(std::round(coord[2]))};
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
			// Check if this child is actually a coordinate array [x,y,z] that we should
			// process at this level rather than recursing into. This handles the case where
			// WKB-decoded boundaries have [x,y,z] coordinate arrays where integer indices
			// would normally be, which adds one nesting level.
			if (depth == 1 && child.is_array() && child.size() == 3 && child[0].is_number() && !child[0].is_array()) {
				// This looks like a coordinate [x,y,z], not a sub-array to recurse into
				// Process it as if we're at depth 0
				std::array<double, 3> coord = {child[0].get<double>(), child[1].get<double>(), child[2].get<double>()};

				std::array<int64_t, 3> quantised;
				if (transform.has_value()) {
					quantised = QuantiseVertex(coord, transform.value());
				} else {
					quantised = {static_cast<int64_t>(std::round(coord[0])), static_cast<int64_t>(std::round(coord[1])),
					             static_cast<int64_t>(std::round(coord[2]))};
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

				child = static_cast<int64_t>(idx);
			} else {
				CollectAndReplaceVertices(child, vertex_map, vertex_pool, transform, depth - 1);
			}
		}
	}
}

// Determine nesting depth of geometry boundaries based on geometry type
static int GetBoundaryDepth(const std::string &geom_type) {
	if (geom_type == "MultiPoint") {
		return 1;
	}
	if (geom_type == "MultiLineString") {
		return 2;
	}
	if (geom_type == "MultiSurface" || geom_type == "CompositeSurface") {
		return 3;
	}
	if (geom_type == "Solid") {
		return 4;
	}
	if (geom_type == "MultiSolid" || geom_type == "CompositeSolid") {
		return 5;
	}
	return 3; // default
}

// ============================================================
// BuildVertexPool
// ============================================================

std::vector<std::array<int64_t, 3>> CityJSONWriter::BuildVertexPool(std::vector<std::pair<std::string, json>> &objects,
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
// BuildMetadataJson
// ============================================================

json CityJSONWriter::BuildMetadataJson(const CityJSONWriteMetadata &metadata) {
	json meta = json::object();

	if (metadata.crs.has_value()) {
		meta["referenceSystem"] = metadata.crs.value();
	}
	if (metadata.title.has_value()) {
		meta["title"] = metadata.title.value();
	}
	if (metadata.identifier.has_value()) {
		meta["identifier"] = metadata.identifier.value();
	}
	if (metadata.reference_date.has_value()) {
		meta["referenceDate"] = metadata.reference_date.value();
	}
	if (metadata.geographical_extent.has_value()) {
		meta["geographicalExtent"] = metadata.geographical_extent->ToJson();
	}
	if (metadata.point_of_contact.has_value()) {
		meta["pointOfContact"] = metadata.point_of_contact->ToJson();
	}

	return meta;
}

// ============================================================
// WriteCityJSON
// ============================================================

void CityJSONWriter::WriteCityJSON(
    const std::string &file_path, const CityJSONWriteMetadata &metadata,
    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
    const std::vector<std::string> &feature_order) {

	// Build the root CityJSON object
	json root;
	root["type"] = "CityJSON";
	root["version"] = metadata.version;

	// Metadata
	auto meta_json = BuildMetadataJson(metadata);
	if (!meta_json.empty()) {
		root["metadata"] = meta_json;
	}

	// Transform
	if (metadata.transform.has_value()) {
		root["transform"] = json::object();
		root["transform"]["scale"] =
		    json::array({metadata.transform->scale[0], metadata.transform->scale[1], metadata.transform->scale[2]});
		root["transform"]["translate"] = json::array(
		    {metadata.transform->translate[0], metadata.transform->translate[1], metadata.transform->translate[2]});
	}

	// Collect all city objects for vertex pool building
	std::vector<std::pair<std::string, json>> all_objects;
	for (const auto &fid : feature_order) {
		auto it = feature_objects.find(fid);
		if (it == feature_objects.end())
			continue;
		for (const auto &[obj_id, obj_json] : it->second) {
			all_objects.emplace_back(obj_id, obj_json);
		}
	}

	// Build global vertex pool (replaces coordinates with indices in-place)
	auto vertex_pool = BuildVertexPool(all_objects, metadata.transform);

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
    const std::string &file_path, const CityJSONWriteMetadata &metadata,
    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
    const std::vector<std::string> &feature_order) {

	std::ofstream out(file_path);
	if (!out.is_open()) {
		throw CityJSONError::FileRead("Failed to open output file: " + file_path);
	}

	// Line 1: metadata header
	json header;
	header["type"] = "CityJSON";
	header["version"] = metadata.version;
	header["CityObjects"] = json::object();
	header["vertices"] = json::array();

	auto meta_json = BuildMetadataJson(metadata);
	if (!meta_json.empty()) {
		header["metadata"] = meta_json;
	}

	if (metadata.transform.has_value()) {
		header["transform"] = json::object();
		header["transform"]["scale"] =
		    json::array({metadata.transform->scale[0], metadata.transform->scale[1], metadata.transform->scale[2]});
		header["transform"]["translate"] = json::array(
		    {metadata.transform->translate[0], metadata.transform->translate[1], metadata.transform->translate[2]});
	}

	out << header.dump() << "\n";

	// Line 2+: one CityJSONFeature per feature_id, with per-feature vertex pool
	for (const auto &fid : feature_order) {
		auto it = feature_objects.find(fid);
		if (it == feature_objects.end())
			continue;

		// Copy objects for this feature (we'll modify them for vertex pool building)
		auto feature_objs = it->second;

		// Build per-feature vertex pool
		auto vertex_pool = BuildVertexPool(feature_objs, metadata.transform);

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

// ============================================================
// WriteFlatCityBuf
// ============================================================

#ifdef CITYJSON_HAS_FCB

void CityJSONWriter::WriteFlatCityBuf(const std::string &file_path, const CityJSONWriteMetadata &metadata,
                                      std::map<std::string, std::vector<std::pair<std::string, json>>> feature_objects,
                                      const std::vector<std::string> &feature_order) {

	// Build the metadata header JSON (same structure as CityJSONSeq line 1)
	json header;
	header["type"] = "CityJSON";
	header["version"] = metadata.version;
	header["CityObjects"] = json::object();
	header["vertices"] = json::array();

	auto meta_json = BuildMetadataJson(metadata);
	if (!meta_json.empty()) {
		header["metadata"] = meta_json;
	}

	// FCB requires a transform field — use identity if none provided
	{
		auto &t = metadata.transform;
		header["transform"] = json::object();
		header["transform"]["scale"] = json::array(
		    {t.has_value() ? t->scale[0] : 1.0, t.has_value() ? t->scale[1] : 1.0, t.has_value() ? t->scale[2] : 1.0});
		header["transform"]["translate"] =
		    json::array({t.has_value() ? t->translate[0] : 0.0, t.has_value() ? t->translate[1] : 0.0,
		                 t.has_value() ? t->translate[2] : 0.0});
	}

	// Create FCB writer with metadata JSON
	std::string header_str = header.dump();
	auto writer = fcb::fcb_writer_new(header_str);

	// Add each feature (same per-feature JSON as CityJSONSeq lines 2+)
	for (const auto &fid : feature_order) {
		auto it = feature_objects.find(fid);
		if (it == feature_objects.end())
			continue;

		// Build per-feature vertex pool (modifies objects in-place)
		auto &feature_objs = it->second;
		auto vertex_pool = BuildVertexPool(feature_objs, metadata.transform);

		// Build CityJSONFeature JSON
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

		std::string feature_str = feature.dump();
		fcb::fcb_writer_add_feature(*writer, feature_str);
	}

	// Write the FCB file to disk
	fcb::fcb_writer_write(std::move(writer), file_path);
}

#endif // CITYJSON_HAS_FCB

} // namespace cityjson
} // namespace duckdb

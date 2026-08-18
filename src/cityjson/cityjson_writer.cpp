#include "cityjson/cityjson_writer.hpp"
#ifdef CITYJSON_HAS_FCB
#include <fcb/writer/attribute.hpp>
#include <fcb/writer/fcb_writer.hpp>
#endif
#include <fstream>
#include <cmath>
#include <map>
#include <algorithm>

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
    const std::vector<std::string> &feature_order, const std::optional<json> &appearance) {
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
		if (it == feature_objects.end()) {
			continue;
		}
		for (const auto &[obj_id, obj_json] : it->second) {
			all_objects.emplace_back(obj_id, obj_json);
		}
	}

	// Build global vertex pool (replaces coordinates with indices in-place)
	auto vertex_pool = BuildVertexPool(all_objects, metadata.transform);

	// CityObjects
	// Definitions for the per-geometry material/texture refs. A whole-document
	// CityJSON has exactly one block, so unlike the Seq path there is nothing to
	// key by feature.
	if (appearance.has_value() && !appearance->empty()) {
		root["appearance"] = appearance.value();
	}

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
		throw CityJSONError::FileWrite("Failed to open output file: " + file_path);
	}
	out << root.dump();
}

// ============================================================
// WriteCityJSONSeq
// ============================================================

void CityJSONWriter::WriteCityJSONSeq(
    const std::string &file_path, const CityJSONWriteMetadata &metadata,
    const std::map<std::string, std::vector<std::pair<std::string, json>>> &feature_objects,
    const std::vector<std::string> &feature_order, const std::optional<json> &appearance_header,
    const std::map<std::string, json> &appearance_by_feature) {
	std::ofstream out(file_path);
	if (!out.is_open()) {
		throw CityJSONError::FileWrite("Failed to open output file: " + file_path);
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

	// The material/texture definitions the per-geometry refs index into. Without
	// them the refs dangle and the output is invalid CityJSON.
	if (appearance_header.has_value() && !appearance_header->empty()) {
		header["appearance"] = appearance_header.value();
	}

	out << header.dump() << "\n";

	// Line 2+: one CityJSONFeature per feature_id, with per-feature vertex pool
	for (const auto &fid : feature_order) {
		auto it = feature_objects.find(fid);
		if (it == feature_objects.end()) {
			continue;
		}

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

		// This feature's own appearance block. Its refs are LOCAL indices into it,
		// so it is re-emitted onto the feature it came from and never merged with
		// another feature's -- merging would preserve every count while silently
		// re-pointing every reference.
		{
			auto appearance_it = appearance_by_feature.find(fid);
			if (appearance_it != appearance_by_feature.end() && !appearance_it->second.empty()) {
				feature["appearance"] = appearance_it->second;
			}
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

namespace {

// Mirrors upstream's write_cityjson.cpp example: everything on a semantic surface
// besides type/parent/children is an indexable "other" attribute.
nlohmann::ordered_json SemanticSurfaceOtherMembers(const nlohmann::ordered_json &surface) {
	nlohmann::ordered_json other = nlohmann::ordered_json::object();
	for (const auto &[key, val] : surface.items()) {
		if (key != "type" && key != "parent" && key != "children") {
			other[key] = val;
		}
	}
	return other;
}

} // namespace

void CityJSONWriter::WriteFlatCityBuf(const std::string &file_path, const CityJSONWriteMetadata &metadata,
                                      std::map<std::string, std::vector<std::pair<std::string, json>>> feature_objects,
                                      const std::vector<std::string> &feature_order,
                                      const std::vector<std::string> &attr_index_columns,
                                      std::optional<uint16_t> branching_factor, std::optional<uint16_t> index_node_size,
                                      const std::vector<std::string> &declared_attr_columns) {
	// Build the metadata header (same shape as CityJSONSeq's line 1).
	json header;
	header["type"] = "CityJSON";
	header["version"] = metadata.version;
	header["CityObjects"] = json::object();
	header["vertices"] = json::array();

	auto meta_json = BuildMetadataJson(metadata);
	if (!meta_json.empty()) {
		header["metadata"] = meta_json;
	}

	// FcbWriter needs a transform to quantize/dequantize vertices -- identity if none given.
	{
		auto &t = metadata.transform;
		header["transform"] = json::object();
		header["transform"]["scale"] = json::array(
		    {t.has_value() ? t->scale[0] : 1.0, t.has_value() ? t->scale[1] : 1.0, t.has_value() ? t->scale[2] : 1.0});
		header["transform"]["translate"] =
		    json::array({t.has_value() ? t->translate[0] : 0.0, t.has_value() ? t->translate[1] : 0.0,
		                 t.has_value() ? t->translate[2] : 0.0});
	}

	// Build each feature's per-feature vertex pool exactly like WriteCityJSONSeq does,
	// and convert both header and features to nlohmann::ordered_json -- the concrete
	// type fcb::add_attributes/FcbWriter require. Since feature_objects' values are
	// plain (map-ordered, i.e. alphabetical) nlohmann::json, converting via dump()+
	// parse() yields alphabetical column-index assignment -- deterministic and
	// correctly self-consistent for round-tripping through our own reader/writer,
	// just not guaranteed byte-identical to what the upstream Rust CLI would produce
	// for the same input (which uses true document/insertion order). That's fine: we
	// don't need CLI byte-compatibility, only correct round-tripping.
	nlohmann::ordered_json ordered_header = nlohmann::ordered_json::parse(header.dump());

	std::vector<nlohmann::ordered_json> ordered_features;
	ordered_features.reserve(feature_order.size());
	for (const auto &fid : feature_order) {
		auto it = feature_objects.find(fid);
		if (it == feature_objects.end()) {
			continue;
		}
		auto &feature_objs = it->second;
		auto vertex_pool = BuildVertexPool(feature_objs, metadata.transform);

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

		ordered_features.push_back(nlohmann::ordered_json::parse(feature.dump()));
	}

	// Pass 1: two-pass attribute schema scan, required before FcbWriter construction
	// because column numbering is assigned as names are first encountered.
	fcb::AttributeSchema attr_schema;
	fcb::AttributeSchema semantic_attr_schema;
	for (const auto &feature : ordered_features) {
		for (const auto &[obj_id, obj] : feature.at("CityObjects").items()) {
			if (auto attr_it = obj.find("attributes"); attr_it != obj.end()) {
				fcb::add_attributes(attr_schema, *attr_it);
			}
			auto geom_it = obj.find("geometry");
			if (geom_it == obj.end() || !geom_it->is_array()) {
				continue;
			}
			for (const auto &geometry : *geom_it) {
				auto sem_it = geometry.find("semantics");
				if (sem_it == geometry.end() || !sem_it->contains("surfaces")) {
					continue;
				}
				for (const auto &surface : sem_it->at("surfaces")) {
					auto other = SemanticSurfaceOtherMembers(surface);
					if (!other.empty()) {
						fcb::add_attributes(semantic_attr_schema, other);
					}
				}
			}
		}
	}
	// Pass 1b: declare attributes that never carry a value.
	//
	// fcb::add_attributes derives each column's type with guess_type, which cannot
	// type a JSON null. Worse, the COPY sink omits null attributes from the JSON
	// altogether, so an attribute that is null in EVERY object is not merely
	// untypeable here -- it is entirely invisible, and the column vanishes from the
	// file. The 3DBAG export has six such columns (eindregistratie,
	// tijdstipinactief, ...), which is why a 74-column CityJSONSeq source came back
	// as 68 columns through FlatCityBuf.
	//
	// The source relation's column list is the authority on which attributes exist,
	// so declare from that rather than from observed values. String is the only
	// honest type for a value we have never seen; the column carries no values
	// either way, so the declared type only affects how it reads back.
	std::uint16_t next_column_index = 0;
	for (const auto &entry : attr_schema) {
		next_column_index =
		    std::max<std::uint16_t>(next_column_index, static_cast<std::uint16_t>(entry.second.first + 1));
	}
	for (const auto &column_name : declared_attr_columns) {
		if (attr_schema.count(column_name) != 0) {
			continue;
		}
		attr_schema.emplace(column_name, std::make_pair(next_column_index++, ::ColumnType::String));
	}

	const bool has_semantic_attrs = !semantic_attr_schema.empty();

	fcb::FcbWriterOptions options;
	if (index_node_size.has_value()) {
		options.index_node_size = index_node_size.value();
	}
	for (const auto &col_name : attr_index_columns) {
		if (attr_schema.count(col_name) == 0) {
			// Requested column never appeared in any feature's attributes -- nothing
			// to index, not an error.
			continue;
		}
		options.attribute_indices.emplace_back(col_name, branching_factor);
	}

	fcb::FcbWriter writer(ordered_header, options, attr_schema,
	                      has_semantic_attrs ? std::optional(semantic_attr_schema) : std::nullopt);
	for (const auto &feature : ordered_features) {
		writer.add_feature(feature);
	}

	std::ofstream out(file_path, std::ios::binary);
	if (!out.is_open()) {
		throw CityJSONError::FileWrite("Failed to open output file: " + file_path);
	}
	writer.write(out); // streaming overload -- bounded memory, unlike write()'s vector return
	out.close();
	if (!out) {
		throw CityJSONError::FileWrite("Failed writing output file: " + file_path);
	}
}

#endif // CITYJSON_HAS_FCB

} // namespace cityjson
} // namespace duckdb

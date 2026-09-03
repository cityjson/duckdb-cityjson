#include "cityjson/obj_reader.hpp"

#include "cityjson/city_object_utils.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/error.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/obj_parser.hpp"
#include "duckdb/logging/logger.hpp"

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>

namespace duckdb {
namespace cityjson {

namespace {

std::string FileStem(const std::string &path) {
	auto base = path.substr(OBJReader::DirectoryOf(path).empty() ? 0 : OBJReader::DirectoryOf(path).size() + 1);
	auto dot = base.rfind('.');
	return dot == std::string::npos ? base : base.substr(0, dot);
}

std::string UpperExtension(const std::string &path) {
	auto dot = path.rfind('.');
	if (dot == std::string::npos) {
		return "";
	}
	std::string ext = path.substr(dot + 1);
	std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::toupper(c); });
	return ext == "JPEG" ? "JPG" : ext;
}

// ------------------------------------------------------------------
// From the parsed document to CityJSON records
// ------------------------------------------------------------------

//! Closed iff every undirected edge is used by exactly two faces.
bool IsClosed(const ObjObject &obj) {
	if (obj.faces.size() < 4) {
		return false;
	}
	std::map<std::pair<int, int>, int> edges;
	for (const auto &face : obj.faces) {
		size_t n = face.v.size();
		for (size_t i = 0; i < n; i++) {
			int a = face.v[i];
			int b = face.v[(i + 1) % n];
			edges[{std::min(a, b), std::max(a, b)}]++;
		}
	}
	return std::all_of(edges.begin(), edges.end(), [](const auto &kv) { return kv.second == 2; });
}

//! Surface type a face carries: its usemtl name when that is a surface type, else its
//! group name when that is, else none.
std::optional<std::string> SurfaceOf(const ObjFace &face) {
	if (OBJReader::IsSemanticSurfaceType(face.usemtl)) {
		return face.usemtl;
	}
	if (OBJReader::IsSemanticSurfaceType(face.group)) {
		return face.group;
	}
	return std::nullopt;
}

} // namespace

// ------------------------------------------------------------------
// OBJReader
// ------------------------------------------------------------------

struct OBJReader::Parsed {
	CityJSON header;
	std::vector<CityJSONFeature> features;
};

OBJReader::OBJReader(ClientContext &context, std::string file_path, OBJReadOptions options)
    : context_(context), file_path_(std::move(file_path)), options_(std::move(options)) {
}

OBJReader::~OBJReader() = default;

std::string OBJReader::Name() const {
	return file_path_;
}

std::string OBJReader::DirectoryOf(const std::string &path) {
	auto slash = path.find_last_of("/\\");
	return slash == std::string::npos ? std::string() : path.substr(0, slash);
}

bool OBJReader::IsSemanticSurfaceType(const std::string &name) {
	static const std::set<std::string> kTypes = {"RoofSurface",
	                                             "WallSurface",
	                                             "GroundSurface",
	                                             "ClosureSurface",
	                                             "OuterCeilingSurface",
	                                             "OuterFloorSurface",
	                                             "Window",
	                                             "Door",
	                                             "InteriorWallSurface",
	                                             "CeilingSurface",
	                                             "FloorSurface",
	                                             "WaterSurface",
	                                             "WaterGroundSurface",
	                                             "TrafficArea",
	                                             "AuxiliaryTrafficArea",
	                                             "TransportationMarking",
	                                             "TransportationHole"};
	if (name.empty()) {
		return false;
	}
	return name[0] == '+' || kTypes.count(name) > 0;
}

std::optional<std::array<double, 3>> OBJReader::ParseOriginComment(const std::string &content) {
	size_t pos = 0;
	while (pos < content.size()) {
		auto eol = content.find('\n', pos);
		auto line = content.substr(pos, eol == std::string::npos ? std::string::npos : eol - pos);
		pos = eol == std::string::npos ? content.size() : eol + 1;
		// A CRLF file's blank line is "\r", not "": without this it reads as the first
		// non-comment line and ends the header before a later `# origin` is reached.
		if (!line.empty() && line.back() == '\r') {
			line.pop_back();
		}
		if (line.rfind("# origin ", 0) != 0) {
			if (!line.empty() && line[0] != '#') {
				return std::nullopt; // the header block is over
			}
			continue;
		}
		std::istringstream in(line.substr(9));
		std::array<double, 3> o {};
		if (in >> o[0] >> o[1] >> o[2]) {
			return o;
		}
		return std::nullopt;
	}
	return std::nullopt;
}

const OBJReader::Parsed &OBJReader::Load() const {
	if (parsed_) {
		return *parsed_;
	}
	std::string content = json_utils::ReadFileContent(context_, file_path_);

	// The `.mtl` files `mtllib` names, resolved against the OBJ's directory and read
	// through DuckDB's FileSystem, so remote paths work. An unreadable one is not fatal:
	// the parser turns the nullopt into a warning and carries on without its materials.
	const std::string base_dir = DirectoryOf(file_path_);
	auto load_mtl = [&](const std::string &mtl_name) -> std::optional<std::string> {
		std::string path = base_dir.empty() ? mtl_name : base_dir + "/" + mtl_name;
		try {
			return json_utils::ReadFileContent(context_, path);
		} catch (const CityJSONError &) {
			return std::nullopt;
		}
	};

	ObjDocument doc;
	try {
		doc = ParseObj(content, FileStem(file_path_), load_mtl);
	} catch (const CityJSONError &e) {
		// The kernel names no file, because it is handed text rather than a path; this
		// is where the path is known, so this is where it is attached.
		throw CityJSONError(e.Kind(), e.what(), file_path_);
	}
	// Everything the parser considers survivable -- an unreadable `mtllib` above all --
	// arrives here and nowhere else. The parse is memoised, so this fires once per reader.
	if (!doc.warnings.empty()) {
		std::string warn;
		for (const auto &line : doc.warnings) {
			warn += line;
			warn += "\n";
		}
		DUCKDB_LOG_WARNING(context_, "read_obj: " + warn);
	}

	// The `# origin` our own writer emits is added back to every vertex, so a round trip
	// through it is exact.
	auto vertices = std::move(doc.vertices);
	if (auto origin = ParseOriginComment(content)) {
		for (auto &v : vertices) {
			v[0] += origin.value()[0];
			v[1] += origin.value()[1];
			v[2] += origin.value()[2];
		}
	}

	auto parsed = std::make_shared<Parsed>();

	// --- Appearance definitions: the MTL materials, and a texture per textured material.
	const auto &materials = doc.materials.materials;
	Appearance appearance;
	std::vector<int> texture_of_material(materials.size(), -1);
	// A directive the `.mtl` does not state stays unset all the way to the sidecar row,
	// where it is SQL NULL: a material with only `Kd` says nothing about its specular
	// colour, and reporting a black one would be inventing a value the file never gave.
	auto colour = [](const std::optional<std::array<double, 3>> &c) -> std::optional<std::vector<double>> {
		std::optional<std::vector<double>> out;
		if (c.has_value()) {
			const auto &v = c.value();
			out = std::vector<double> {v[0], v[1], v[2]};
		}
		return out;
	};
	for (size_t i = 0; i < materials.size(); i++) {
		const auto &m = materials[i];
		Material mat;
		mat.name = m.name;
		mat.diffuse_color = colour(m.diffuse);
		mat.specular_color = colour(m.specular);
		mat.emissive_color = colour(m.emission);
		if (m.ambient.has_value()) {
			const auto &ka = m.ambient.value();
			mat.ambient_intensity = (ka[0] + ka[1] + ka[2]) / 3.0;
		}
		mat.transparency = m.transparency;
		if (m.shininess.has_value()) {
			// MTL's Ns runs to 1000; CityJSON's shininess is [0, 1].
			mat.shininess = std::min(1.0, std::max(0.0, m.shininess.value() / 1000.0));
		}
		if (!m.other.empty()) {
			mat.other = json::object();
			for (const auto &kv : m.other) {
				mat.other[kv.first] = kv.second;
			}
		}
		appearance.materials.push_back(std::move(mat));
		if (!m.map_kd.empty()) {
			Texture tex;
			tex.image_uri = m.map_kd;
			tex.image_type = UpperExtension(m.map_kd);
			tex.wrap_mode = "wrap";
			tex.texture_type = "unknown";
			texture_of_material[i] = static_cast<int>(appearance.textures.size());
			appearance.textures.push_back(std::move(tex));
		}
	}
	appearance.vertices_texture = doc.texcoords;
	if (!appearance.Empty()) {
		parsed->header.appearance = appearance;
	}

	// --- Extent over every vertex, for obj_metadata.
	if (!vertices.empty()) {
		GeographicalExtent extent(vertices[0][0], vertices[0][1], vertices[0][2], vertices[0][0], vertices[0][1],
		                          vertices[0][2]);
		for (const auto &v : vertices) {
			extent = extent.Union(GeographicalExtent(v[0], v[1], v[2], v[0], v[1], v[2]));
		}
		Metadata md;
		md.geographical_extent = extent;
		parsed->header.metadata = md;
	}
	if (options_.crs.has_value()) {
		if (!parsed->header.metadata.has_value()) {
			parsed->header.metadata = Metadata();
		}
		parsed->header.metadata->reference_system = options_.crs.value();
	}

	// --- One feature per object.
	const bool force_solid = options_.geometry_type == "Solid";
	const bool force_multi = options_.geometry_type == "MultiSurface";
	for (const auto &obj : doc.objects) {
		if (obj.faces.empty()) {
			continue;
		}
		CityJSONFeature feature(obj.name);
		std::unordered_map<int, int> local_of_global;
		auto local_index = [&](int g) {
			auto it = local_of_global.find(g);
			if (it != local_of_global.end()) {
				return it->second;
			}
			int idx = static_cast<int>(feature.vertices.size());
			feature.vertices.push_back(vertices[g]);
			local_of_global.emplace(g, idx);
			return idx;
		};

		const bool solid = force_solid || (!force_multi && IsClosed(obj));

		json faces = json::array();      // face -> [ring]
		json sem_values = json::array(); // face -> surface index | null
		json mat_values = json::array(); // face -> material id | null
		json tex_values = json::array(); // face -> [ [texId, uv...] ]
		std::vector<std::string> surfaces;
		bool any_surface = false;
		bool any_material = false;
		bool any_texture = false;

		for (const auto &face : obj.faces) {
			json ring = json::array();
			for (int g : face.v) {
				ring.push_back(local_index(g));
			}
			faces.push_back(json::array({ring}));

			if (auto surface = SurfaceOf(face)) {
				auto it = std::find(surfaces.begin(), surfaces.end(), surface.value());
				if (it == surfaces.end()) {
					surfaces.push_back(surface.value());
					it = surfaces.end() - 1;
				}
				sem_values.push_back(static_cast<int>(it - surfaces.begin()));
				any_surface = true;
			} else {
				sem_values.push_back(nullptr);
			}

			if (face.material_index >= 0) {
				mat_values.push_back(face.material_index);
				any_material = true;
			} else {
				mat_values.push_back(nullptr);
			}

			int tex = face.material_index >= 0 ? texture_of_material[face.material_index] : -1;
			bool has_uv = std::all_of(face.vt.begin(), face.vt.end(), [](int i) { return i >= 0; });
			if (tex >= 0 && has_uv) {
				json tring = json::array({tex});
				for (int vt : face.vt) {
					tring.push_back(vt);
				}
				tex_values.push_back(json::array({tring}));
				any_texture = true;
			} else {
				tex_values.push_back(json::array({json::array({nullptr})}));
			}
		}

		Geometry geom;
		geom.lod = options_.lod;
		if (solid) {
			geom.type = "Solid";
			geom.boundaries = json::array({faces});
		} else {
			geom.type = "MultiSurface";
			geom.boundaries = faces;
		}
		auto nest = [&](json values) {
			return solid ? json::array({std::move(values)}) : values;
		};
		if (any_surface) {
			json surfs = json::array();
			for (const auto &s : surfaces) {
				surfs.push_back(json {{"type", s}});
			}
			geom.semantics = json {{"surfaces", surfs}, {"values", nest(sem_values)}};
		}
		if (any_material) {
			geom.material = json {{"visual", json {{"values", nest(mat_values)}}}};
		}
		if (any_texture) {
			geom.texture = json {{"visual", json {{"values", nest(tex_values)}}}};
		}

		CityObject city_object(options_.object_type);
		city_object.feature_id = obj.name;
		city_object.geometry.push_back(std::move(geom));
		feature.city_objects[obj.name] = std::move(city_object);
		parsed->features.push_back(std::move(feature));
	}

	parsed_ = parsed;
	return *parsed_;
}

CityJSON OBJReader::ReadMetadata() const {
	return Load().header;
}

CityJSONFeatureChunk OBJReader::ReadAllChunks() const {
	auto features = Load().features;
	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

std::vector<CityJSONFeature> OBJReader::ReadNFeatures(size_t n) const {
	const auto &features = Load().features;
	auto count = static_cast<std::ptrdiff_t>(std::min(n, features.size()));
	return std::vector<CityJSONFeature>(features.begin(), features.begin() + count);
}

size_t OBJReader::CountCityObjects() const {
	return Load().features.size();
}

size_t OBJReader::CountFeatures() const {
	return Load().features.size();
}

std::vector<Column> OBJReader::Columns() const {
	if (cached_columns_.has_value()) {
		return cached_columns_.value();
	}
	// Same order as LocalCityJSONReader::Columns: head, bbox + geometry group, trailing.
	// No attribute columns: an OBJ carries none.
	std::vector<Column> columns = GetDefinedColumns();
	const auto &features = Load().features;
	auto geom_columns = CityObjectUtils::InferGeometryColumns(features, features.size());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());
	auto trailing = LODTableUtils::GetTrailingColumns();
	columns.insert(columns.end(), trailing.begin(), trailing.end());
	cached_columns_ = columns;
	return columns;
}

} // namespace cityjson
} // namespace duckdb

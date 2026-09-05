#include "cityjson/mesh_model.hpp"

#include <algorithm>
#include <functional>
#include <limits>
#include <sstream>
#include <tuple>

namespace duckdb {
namespace cityjson {

namespace {

// How many array levels sit above a face for each CityJSON geometry type.
int FaceDepth(const std::string &type) {
	if (type == "MultiSurface" || type == "CompositeSurface") {
		return 0;
	}
	if (type == "Solid") {
		return 1;
	}
	if (type == "MultiSolid" || type == "CompositeSolid") {
		return 2;
	}
	return -1; // points and lines are not renderable surfaces
}

const json &EmptyJson() {
	static const json null_value;
	return null_value;
}

// Walk `boundaries` down to face level, with `sem` (always nested -- the COPY sink
// re-nests semantics.values to match `boundaries`) kept in step. material_lod*/
// texture_lod* stay flat regardless of nesting (spec §04, "material / texture
// columns"): one entry per face, aligned to the WKB face order the same
// `face_counter` counts here, so the caller indexes them directly by face position
// rather than walking them alongside `boundaries`. `sem` may be null or shorter than
// the boundaries; a missing entry reads as null.
void ForEachFace(const json &boundaries, const json &sem, int depth,
                 const std::function<void(const json &face, const json &sem_v)> &fn) {
	auto at = [](const json &arr, size_t i) -> const json & {
		return arr.is_array() && i < arr.size() ? arr[i] : EmptyJson();
	};
	if (depth == 0) {
		if (!boundaries.is_array()) {
			return;
		}
		for (size_t i = 0; i < boundaries.size(); i++) {
			fn(boundaries[i], at(sem, i));
		}
		return;
	}
	if (!boundaries.is_array()) {
		return;
	}
	for (size_t i = 0; i < boundaries.size(); i++) {
		ForEachFace(boundaries[i], at(sem, i), depth - 1, fn);
	}
}

// The first theme's flat `values` (`{theme: {"values": [...]}}`, appearance_cell.hpp's
// `*CellToFlatJson` shape -- the only shape a material_lod*/texture_lod* MAP column
// produces). nlohmann keeps object keys sorted, so "first" means alphabetically first
// by theme name -- a source with only one theme (the common case) is unaffected.
const json &FirstThemeValues(const json &themed) {
	if (!themed.is_object() || themed.empty()) {
		return EmptyJson();
	}
	const json &theme = themed.begin().value();
	if (theme.is_object()) {
		auto v = theme.find("values");
		if (v != theme.end()) {
			return *v;
		}
	}
	return EmptyJson();
}

double LodNumber(const std::string &lod) {
	try {
		return std::stod(lod);
	} catch (...) {
		return -1.0;
	}
}

} // namespace

std::optional<Vertex3> ParseOriginOption(const std::string &text) {
	std::string s = text;
	std::replace(s.begin(), s.end(), ',', ' ');
	std::istringstream in(s);
	Vertex3 o {};
	if (in >> o[0] >> o[1] >> o[2]) {
		return o;
	}
	return std::nullopt;
}

std::array<double, 3> DefaultColour(const std::string &surface_type, const std::string &object_type) {
	// Up3date's semantic palette, then cjio's per-class palette.
	static const std::map<std::string, std::array<double, 3>> by_surface = {
	    {"RoofSurface", {0.9, 0.06, 0.09}},
	    {"WallSurface", {0.8, 0.8, 0.8}},
	    {"GroundSurface", {0.35, 0.35, 0.35}},
	    {"ClosureSurface", {0.6, 0.6, 0.7}},
	    {"OuterCeilingSurface", {0.7, 0.7, 0.75}},
	    {"OuterFloorSurface", {0.5, 0.5, 0.5}},
	    {"Window", {0.4, 0.55, 0.75}},
	    {"Door", {0.45, 0.3, 0.2}},
	    {"InteriorWallSurface", {0.9, 0.9, 0.85}},
	    {"CeilingSurface", {0.95, 0.95, 0.95}},
	    {"FloorSurface", {0.6, 0.55, 0.5}},
	    {"WaterSurface", {0.3, 0.55, 0.85}},
	    {"WaterGroundSurface", {0.25, 0.35, 0.5}},
	    {"TrafficArea", {0.4, 0.4, 0.4}},
	    {"AuxiliaryTrafficArea", {0.55, 0.55, 0.5}}};
	static const std::map<std::string, std::array<double, 3>> by_class = {
	    {"Building", {0.72, 0.32, 0.22}},
	    {"BuildingPart", {0.72, 0.32, 0.22}},
	    {"BuildingInstallation", {0.72, 0.32, 0.22}},
	    {"Road", {0.5, 0.5, 0.5}},
	    {"Railway", {0.4, 0.4, 0.4}},
	    {"TransportSquare", {0.55, 0.55, 0.55}},
	    {"WaterBody", {0.3, 0.7, 0.9}},
	    {"PlantCover", {0.35, 0.65, 0.3}},
	    {"SolitaryVegetationObject", {0.3, 0.6, 0.25}},
	    {"LandUse", {0.85, 0.8, 0.4}},
	    {"Bridge", {0.6, 0.4, 0.7}},
	    {"BridgePart", {0.6, 0.4, 0.7}},
	    {"Tunnel", {0.15, 0.15, 0.15}},
	    {"TunnelPart", {0.15, 0.15, 0.15}},
	    {"TINRelief", {0.55, 0.4, 0.25}},
	    {"GenericCityObject", {0.9, 0.5, 0.7}}};
	auto s = by_surface.find(surface_type);
	if (s != by_surface.end()) {
		return s->second;
	}
	auto c = by_class.find(object_type);
	if (c != by_class.end()) {
		return c->second;
	}
	return {0.6, 0.6, 0.6};
}

bool MaterialIdentity::operator<(const MaterialIdentity &other) const {
	return std::tie(kind, material_id, label, texture_id) <
	       std::tie(other.kind, other.material_id, other.label, other.texture_id);
}

MaterialIdentity IdentityOf(const MeshObject &object, const MeshFace &face, const AppearanceSource &appearance,
                            int64_t texture_id) {
	MaterialIdentity key;
	key.texture_id = texture_id;
	const bool has_material =
	    face.material >= 0 && appearance.Materials().find(face.material) != appearance.Materials().end();
	const std::string surface = face.surface >= 0 && static_cast<size_t>(face.surface) < object.surfaces.size()
	                                ? object.surfaces[face.surface]
	                                : "";
	if (has_material) {
		key.kind = MaterialKind::Material;
		key.material_id = face.material;
	} else if (!surface.empty()) {
		key.kind = MaterialKind::DefaultSurface;
		key.label = surface;
	} else {
		key.kind = MaterialKind::DefaultClass;
		key.label = object.object_type;
	}
	return key;
}

std::string FaceGroupName(const MeshObject &object, const MeshFace &face, const AppearanceSource &appearance) {
	if (face.material >= 0) {
		auto it = appearance.Materials().find(face.material);
		if (it != appearance.Materials().end() && !it->second.name.empty()) {
			return it->second.name;
		}
	}
	if (face.surface >= 0 && static_cast<size_t>(face.surface) < object.surfaces.size()) {
		return object.surfaces[face.surface];
	}
	return object.object_type;
}

MeshModel BuildMeshModel(const std::map<std::string, std::vector<std::pair<std::string, json>>> &objects,
                         const std::vector<std::string> &feature_order, const AppearanceSource &appearance,
                         const MeshBuildOptions &options, const std::optional<std::string> &crs) {
	MeshModel model;
	model.crs = crs;

	for (const auto &feature_id : feature_order) {
		auto fit = objects.find(feature_id);
		if (fit == objects.end()) {
			continue;
		}
		for (const auto &entry : fit->second) {
			const std::string &id = entry.first;
			const json &city_obj = entry.second;
			auto geoms = city_obj.find("geometry");
			if (geoms == city_obj.end() || !geoms->is_array() || geoms->empty()) {
				model.warnings.push_back("object " + id + ": no geometry");
				continue;
			}

			// Choose the geometry: the requested LoD, else the highest one that is a
			// surface. An unparseable/absent lod ranks lowest -- still chosen over
			// nothing, but never over a geometry whose lod did parse.
			const json *chosen = nullptr;
			double best = -std::numeric_limits<double>::infinity();
			for (const auto &g : *geoms) {
				std::string lod = g.value("lod", "");
				std::string type = g.value("type", "");
				if (FaceDepth(type) < 0) {
					continue;
				}
				if (options.lod.has_value()) {
					if (lod == options.lod.value()) {
						chosen = &g;
						break;
					}
					continue;
				}
				double n = LodNumber(lod);
				if (n > best) {
					best = n;
					chosen = &g;
				}
			}
			if (chosen == nullptr) {
				model.warnings.push_back(options.lod.has_value()
				                             ? ("object " + id + ": no geometry at lod '" + *options.lod + "'")
				                             : ("object " + id + ": no renderable geometry"));
				continue;
			}
			const json &geom = *chosen;
			const int face_depth = FaceDepth(geom.value("type", ""));

			MeshObject object;
			object.id = id;
			object.object_type = city_obj.value("type", "");
			object.lod = geom.value("lod", "");
			auto attrs = city_obj.find("attributes");
			object.attributes = attrs != city_obj.end() ? *attrs : json(nullptr);

			// Semantic surfaces: the type of each entry, in the source's order.
			json sem_values;
			if (geom.contains("semantics") && geom["semantics"].is_object()) {
				const json &sem = geom["semantics"];
				if (sem.contains("surfaces") && sem["surfaces"].is_array()) {
					for (const auto &s : sem["surfaces"]) {
						object.surfaces.push_back(s.is_object() ? s.value("type", "") : "");
					}
				}
				if (sem.contains("values")) {
					sem_values = sem["values"];
				}
			}
			// material_lod*/texture_lod* MAP columns always give a flat, WKB-face-aligned
			// `values` (appearance_cell.hpp's `*CellToFlatJson` shape) -- indexed below by
			// `this_face`, the same order `ForEachFace` counts boundaries in.
			json mat_values = geom.contains("material") ? FirstThemeValues(geom["material"]) : json();
			json tex_values = geom.contains("texture") ? FirstThemeValues(geom["texture"]) : json();

			std::map<std::array<double, 3>, uint32_t> vertex_index;
			bool legacy_index_boundaries = false; // boundaries leaves are indices, not [x,y,z]
			auto add_vertex = [&](const json &p) -> std::optional<uint32_t> {
				if (!p.is_array() || p.size() < 3) {
					if (p.is_number_integer()) {
						legacy_index_boundaries = true;
					}
					return std::nullopt;
				}
				Vertex3 v {p[0].get<double>(), p[1].get<double>(), p[2].get<double>()};
				auto it = vertex_index.find(v);
				if (it != vertex_index.end()) {
					return it->second;
				}
				auto idx = static_cast<uint32_t>(object.vertices.size());
				object.vertices.push_back(v);
				vertex_index.emplace(v, idx);
				return idx;
			};

			auto bit = geom.find("boundaries");
			if (bit == geom.end()) {
				model.warnings.push_back("object " + id + ": geometry has no boundaries");
				continue;
			}

			size_t face_counter = 0;
			bool warned_bad_face = false;
			bool warned_degenerate_face = false;
			ForEachFace(*bit, sem_values, face_depth, [&](const json &face_json, const json &sem_v) {
				const size_t this_face = face_counter++;
				MeshFace face;
				if (!face_json.is_array()) {
					if (!warned_bad_face) {
						model.warnings.push_back("object " + id + ": a face is not an array, skipped");
						warned_bad_face = true;
					}
					return;
				}
				for (size_t r = 0; r < face_json.size(); r++) {
					std::vector<uint32_t> ring;
					for (const auto &p : face_json[r]) {
						if (auto idx = add_vertex(p)) {
							ring.push_back(*idx);
						}
					}
					if (ring.size() >= 3) {
						face.rings.push_back(std::move(ring));
					} else if (r == 0) {
						if (!legacy_index_boundaries && !warned_degenerate_face) {
							model.warnings.push_back(
							    "object " + id +
							    ": a face's outer ring has fewer than three resolvable vertices, skipped");
							warned_degenerate_face = true;
						}
						return; // an outer ring with under three vertices is not a face
					}
				}
				if (face.rings.empty()) {
					return; // a face with no ring entries at all
				}
				if (sem_v.is_number_integer()) {
					face.surface = sem_v.get<int32_t>();
				}

				// Material: mat_values indexed by face position.
				int64_t mat_ref = -1;
				if (this_face < mat_values.size() && mat_values[this_face].is_number_integer()) {
					mat_ref = mat_values[this_face].get<int64_t>();
				}
				if (mat_ref >= 0) {
					if (auto mat_id = appearance.ResolveMaterial(feature_id, mat_ref)) {
						face.material = *mat_id;
					}
				}

				// Texture: tex_values indexed by face position -- a list over the
				// face's rings, each ring [texId, uv, uv, ...]; the id is the first
				// ring's.
				const json *tex_node = this_face < tex_values.size() ? &tex_values[this_face] : nullptr;
				if (tex_node != nullptr && tex_node->is_array() && !tex_node->empty() && (*tex_node)[0].is_array() &&
				    !(*tex_node)[0].empty() && (*tex_node)[0][0].is_number_integer()) {
					// The UVs are read before the texture id is resolved, because
					// reading them is what enforces spec rule 1: sidecar form
					// inlines every pair, so AppearanceSource::UV throws on an
					// index. materials_query on its own declares the form and
					// defines no texture at all -- exactly the case where local
					// indices would otherwise be resolved as global ids -- so the
					// refusal must not wait on an id resolving.
					std::vector<std::vector<std::array<double, 2>>> uvs;
					bool complete = true;
					for (size_t r = 0; r < face.rings.size() && complete; r++) {
						const json &tring = r < tex_node->size() ? (*tex_node)[r] : EmptyJson();
						std::vector<std::array<double, 2>> ring_uv;
						for (size_t k = 1; tring.is_array() && k < tring.size(); k++) {
							if (auto uv = appearance.UV(feature_id, tring[k])) {
								ring_uv.push_back(*uv);
							}
						}
						complete = ring_uv.size() == face.rings[r].size();
						uvs.push_back(std::move(ring_uv));
					}
					auto tid = appearance.ResolveTexture(feature_id, (*tex_node)[0][0].get<int64_t>());
					if (tid.has_value() && complete) {
						face.texture = *tid;
						face.uvs = std::move(uvs);
					}
				}
				object.faces.push_back(std::move(face));
			});

			if (legacy_index_boundaries) {
				model.warnings.push_back("object " + id +
				                         ": boundaries are vertex indices, not [x,y,z] coordinates (the legacy "
				                         "geom_lod* STRUCT layout is not supported by the mesh writers)");
			}
			if (object.faces.empty()) {
				continue;
			}
			for (const auto &v : object.vertices) {
				GeographicalExtent e(v[0], v[1], v[2], v[0], v[1], v[2]);
				model.extent = model.extent.has_value() ? model.extent->Union(e) : e;
			}
			model.objects.push_back(std::move(object));
		}
	}

	if (options.origin == "none") {
		model.origin = {0.0, 0.0, 0.0};
	} else if (options.origin == "auto") {
		if (model.extent.has_value()) {
			model.origin = {model.extent->min_x, model.extent->min_y, model.extent->min_z};
		}
	} else if (auto o = ParseOriginOption(options.origin)) {
		model.origin = *o;
	}
	return model;
}

} // namespace cityjson
} // namespace duckdb

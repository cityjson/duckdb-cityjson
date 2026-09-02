#include "cityjson/obj_writer.hpp"

#include "cityjson/error.hpp"
#include "cityjson/face_triangulation.hpp"

#include <array>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <map>
#include <optional>
#include <tuple>
#include <vector>

namespace duckdb {
namespace cityjson {

std::string FormatDouble(double v, int precision) {
	char buf[64];
	if (precision >= 17) {
		// Shortest text that round-trips: 15 digits when that reads back exactly, 17 otherwise.
		std::snprintf(buf, sizeof(buf), "%.15g", v);
		if (std::strtod(buf, nullptr) != v) {
			std::snprintf(buf, sizeof(buf), "%.17g", v);
		}
	} else {
		std::snprintf(buf, sizeof(buf), "%.*g", precision < 1 ? 1 : precision, v);
	}
	return buf;
}

namespace {

// Everything an .mtl entry needs, keyed by the usemtl name the OBJ uses.
struct MtlEntry {
	std::array<double, 3> kd {};
	std::optional<std::array<double, 3>> ks;
	std::optional<std::array<double, 3>> ke;
	double d = 1.0;
	std::optional<double> ns;
	std::string map_kd; // empty = none
};

// What a .mtl entry's contents actually depend on -- not the rendered usemtl name,
// which two distinct identities can share (a CityJSON material named "RoofSurface"
// and a default-coloured "RoofSurface" surface; two materials sharing a name).
enum class MtlKind { Material, DefaultSurface, DefaultClass };

struct MtlKey {
	MtlKind kind = MtlKind::DefaultClass;
	int64_t material_id = -1; // Kind::Material
	std::string label;        // surface type (DefaultSurface) or object class (DefaultClass)
	int64_t texture_id = -1;  // -1 = untextured

	bool operator<(const MtlKey &other) const {
		return std::tie(kind, material_id, label, texture_id) <
		       std::tie(other.kind, other.material_id, other.label, other.texture_id);
	}
};

std::string Sanitise(const std::string &name) {
	// OBJ tokens are whitespace-delimited; keep names to one token.
	std::string out = name;
	for (auto &c : out) {
		if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
			c = '_';
		}
	}
	return out.empty() ? "unnamed" : out;
}

} // namespace

void WriteOBJ(const MeshModel &model, AppearanceSource &appearance, const std::string &obj_path,
              const std::string &mtl_path, const std::string &mtl_basename, const OBJWriteOptions &options,
              const std::function<bool(int64_t texture_id, std::string &basename)> &image_writer,
              std::vector<std::string> &warnings) {
	std::ofstream out(obj_path);
	if (!out.is_open()) {
		throw CityJSONError::FileWrite("Failed to open output file: " + obj_path);
	}

	std::map<std::string, MtlEntry> mtl;           // rendered usemtl name -> .mtl contents
	std::vector<std::string> mtl_order;            // deterministic .mtl order
	std::map<MtlKey, std::string> key_to_name;     // identity -> the name already assigned to it
	std::map<int64_t, std::string> image_basename; // texture id -> copied file, "" = failed
	auto texture_file = [&](int64_t tex) -> std::string {
		auto it = image_basename.find(tex);
		if (it != image_basename.end()) {
			return it->second;
		}
		std::string basename;
		bool ok = image_writer(tex, basename);
		image_basename[tex] = ok ? basename : "";
		return image_basename[tex];
	};
	// The usemtl name of a face, registering its .mtl entry on first use. A textured
	// face gets its own entry (OBJ ties map_Kd to the material; CityJSON does not).
	// Entries are keyed by identity (MtlKey), not by the rendered name string, so two
	// distinct identities that render to the same name (a material and a default-coloured
	// surface sharing a label, or two materials sharing a name) get distinct .mtl entries.
	auto material_name = [&](const MeshObject &object, const MeshFace &face) -> std::string {
		std::string base = Sanitise(FaceGroupName(object, face, appearance));
		std::string map;
		if (face.texture >= 0) {
			map = texture_file(face.texture);
		}

		MtlKey key;
		// A texture whose bytes could not be had leaves the face with its colour and no
		// map_Kd -- which is what an untextured face writes -- so it must not key an
		// entry of its own, or the same contents are written twice under two names.
		key.texture_id = map.empty() ? -1 : face.texture;
		auto mit = face.material >= 0 ? appearance.Materials().find(face.material) : appearance.Materials().end();
		bool has_material = mit != appearance.Materials().end();
		if (has_material) {
			key.kind = MtlKind::Material;
			key.material_id = face.material;
		} else {
			std::string surface = face.surface >= 0 && static_cast<size_t>(face.surface) < object.surfaces.size()
			                          ? object.surfaces[face.surface]
			                          : "";
			if (!surface.empty()) {
				key.kind = MtlKind::DefaultSurface;
				key.label = surface;
			} else {
				key.kind = MtlKind::DefaultClass;
				key.label = object.object_type;
			}
		}

		auto existing = key_to_name.find(key);
		if (existing != key_to_name.end()) {
			return existing->second;
		}

		std::string name = map.empty() ? base : base + "__tex" + std::to_string(face.texture);
		if (mtl.count(name) != 0) {
			// The name string is already claimed by a different identity -- disambiguate.
			name += key.kind == MtlKind::Material ? "_" + std::to_string(key.material_id) : "_default";
		}

		MtlEntry e;
		if (has_material) {
			e.kd = mit->second.diffuse;
			e.ks = mit->second.specular;
			e.ke = mit->second.emissive;
			e.d = 1.0 - mit->second.transparency;
			if (mit->second.shininess.has_value()) {
				e.ns = mit->second.shininess.value() * 1000.0;
			}
		} else {
			e.kd = DefaultColour(key.label, object.object_type);
		}
		e.map_kd = map;
		mtl[name] = e;
		mtl_order.push_back(name);
		key_to_name[key] = name;
		return name;
	};

	out << "# Written by duckdb-cityjson\n";
	if (model.crs.has_value()) {
		out << "# crs " << model.crs.value() << "\n";
	}
	const auto &o = model.origin;
	if (o[0] != 0.0 || o[1] != 0.0 || o[2] != 0.0) {
		out << "# origin " << FormatDouble(o[0], 17) << " " << FormatDouble(o[1], 17) << " " << FormatDouble(o[2], 17)
		    << "\n";
	}
	out << "mtllib " << mtl_basename << "\n";

	size_t v_base = 0;  // vertices written so far (OBJ indices are global and 1-based)
	size_t vt_base = 0; // texture coordinates written so far
	for (const auto &object : model.objects) {
		out << "o " << Sanitise(object.id) << "\n";
		for (const auto &v : object.vertices) {
			out << "v " << FormatDouble(v[0] - o[0], options.precision) << " "
			    << FormatDouble(v[1] - o[1], options.precision) << " " << FormatDouble(v[2] - o[2], options.precision)
			    << "\n";
		}
		// Per-object UV pool, deduplicated exactly.
		std::map<std::array<double, 2>, size_t> uv_index;
		std::vector<std::array<double, 2>> uv_list;
		auto uv_of = [&](const std::array<double, 2> &uv) {
			auto it = uv_index.find(uv);
			if (it != uv_index.end()) {
				return it->second;
			}
			uv_list.push_back(uv);
			uv_index.emplace(uv, uv_list.size());
			return uv_list.size(); // 1-based within this object
		};
		// Face lines are buffered so `vt` lines can precede them. `g`/`usemtl` are OBJ
		// file-global state that survives an `o` line, so both are reset to an
		// impossible sentinel per object: every object's first face re-emits both,
		// rather than silently inheriting the previous object's trailing state.
		std::string faces;
		std::string current_group = "\x01";
		std::string current_material = "\x01";
		for (size_t face_ordinal = 0; face_ordinal < object.faces.size(); face_ordinal++) {
			const auto &face = object.faces[face_ordinal];
			std::string surface_type = face.surface >= 0 && static_cast<size_t>(face.surface) < object.surfaces.size()
			                               ? Sanitise(object.surfaces[face.surface])
			                               : "";
			std::string group = surface_type.empty() ? std::string("default") : surface_type;
			if (group != current_group) {
				faces += "g " + group + "\n";
				current_group = group;
			}
			std::string mat = material_name(object, face);
			if (mat != current_material) {
				faces += "usemtl " + mat + "\n";
				current_material = mat;
			}
			const bool textured = !face.uvs.empty() && face.uvs.size() == face.rings.size();
			auto emit_polygon = [&](const std::vector<uint32_t> &ring, const std::vector<std::array<double, 2>> *uvs) {
				faces += "f";
				for (size_t k = 0; k < ring.size(); k++) {
					faces += " " + std::to_string(v_base + ring[k] + 1);
					if (uvs != nullptr) {
						faces += "/" + std::to_string(vt_base + uv_of((*uvs)[k]));
					}
				}
				faces += "\n";
			};
			if (face.rings.size() > 1 || options.triangulate) {
				auto tris = TriangulateFace(object.vertices, face.rings);
				if (tris.empty()) {
					// A face whose outer ring has no plane normal (every vertex collinear,
					// or coincident) cannot be triangulated, and OBJ has no way to say so.
					warnings.push_back("object " + object.id + ": face " + std::to_string(face_ordinal) +
					                   " has no plane normal and could not be triangulated, skipped");
					continue;
				}
				// UV per vertex index (a vertex has one UV within a face).
				std::map<uint32_t, std::array<double, 2>> uv_at;
				if (textured) {
					for (size_t r = 0; r < face.rings.size(); r++) {
						for (size_t k = 0; k < face.rings[r].size(); k++) {
							uv_at[face.rings[r][k]] = face.uvs[r][k];
						}
					}
				}
				for (size_t t = 0; t + 2 < tris.size(); t += 3) {
					std::vector<uint32_t> tri {tris[t], tris[t + 1], tris[t + 2]};
					if (textured) {
						auto u0 = uv_at.find(tri[0]);
						auto u1 = uv_at.find(tri[1]);
						auto u2 = uv_at.find(tri[2]);
						if (u0 == uv_at.end() || u1 == uv_at.end() || u2 == uv_at.end()) {
							// A triangulated vertex has no recorded UV -- skip rather than
							// insert a fabricated {0, 0} and silently mis-texture the face.
							continue;
						}
						std::vector<std::array<double, 2>> tuv {u0->second, u1->second, u2->second};
						emit_polygon(tri, &tuv);
					} else {
						emit_polygon(tri, nullptr);
					}
				}
			} else if (face.rings[0].size() >= 3) {
				emit_polygon(face.rings[0], textured ? &face.uvs[0] : nullptr);
			}
		}
		for (const auto &uv : uv_list) {
			out << "vt " << FormatDouble(uv[0], options.precision) << " " << FormatDouble(uv[1], options.precision)
			    << "\n";
		}
		out << faces;
		v_base += object.vertices.size();
		vt_base += uv_list.size();
	}
	out.close();
	if (!out) {
		throw CityJSONError::FileWrite("Failed writing output file: " + obj_path);
	}

	std::ofstream mtl_out(mtl_path);
	if (!mtl_out.is_open()) {
		throw CityJSONError::FileWrite("Failed to open output file: " + mtl_path);
	}
	mtl_out << "# Written by duckdb-cityjson\n";
	for (const auto &name : mtl_order) {
		const auto &e = mtl[name];
		mtl_out << "newmtl " << name << "\n";
		mtl_out << "Kd " << FormatDouble(e.kd[0], 6) << " " << FormatDouble(e.kd[1], 6) << " "
		        << FormatDouble(e.kd[2], 6) << "\n";
		if (e.ks.has_value()) {
			mtl_out << "Ks " << FormatDouble((*e.ks)[0], 6) << " " << FormatDouble((*e.ks)[1], 6) << " "
			        << FormatDouble((*e.ks)[2], 6) << "\n";
		}
		if (e.ke.has_value()) {
			mtl_out << "Ke " << FormatDouble((*e.ke)[0], 6) << " " << FormatDouble((*e.ke)[1], 6) << " "
			        << FormatDouble((*e.ke)[2], 6) << "\n";
		}
		mtl_out << "d " << FormatDouble(e.d, 6) << "\n";
		if (e.ns.has_value()) {
			mtl_out << "Ns " << FormatDouble(*e.ns, 6) << "\n";
		}
		if (!e.map_kd.empty()) {
			mtl_out << "map_Kd " << e.map_kd << "\n";
		}
		mtl_out << "\n";
	}
	mtl_out.close();
	if (!mtl_out) {
		throw CityJSONError::FileWrite("Failed writing output file: " + mtl_path);
	}
}

} // namespace cityjson
} // namespace duckdb

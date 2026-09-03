#pragma once

#include <array>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace duckdb {
namespace cityjson {

//! One `f` line: the resolved corners plus the `usemtl` / `g` state that was in force.
struct ObjFace {
	std::vector<int> v;      // 0-based indices into ObjDocument::vertices
	std::vector<int> vt;     // 0-based indices into ObjDocument::texcoords, -1 = none
	std::string usemtl;      // the `usemtl` name in force, "" = none
	std::string group;       // the last `g` name in force, "" = none
	int material_index = -1; // into ObjDocument::materials, -1 = no such material
};

//! One `o` block. A repeated `o` name resumes its object, so this holds every face
//! declared under the name, in file order.
struct ObjObject {
	std::string name;
	std::vector<ObjFace> faces;
};

//! What one `newmtl` block states. Absence is the point of the optionals: a block that
//! declares only `Kd` says nothing about its specular colour, and a zero-initialised
//! record cannot tell that apart from a black one.
struct MtlMaterial {
	std::string name;
	std::optional<std::array<double, 3>> ambient;  // Ka
	std::optional<std::array<double, 3>> diffuse;  // Kd
	std::optional<std::array<double, 3>> specular; // Ks
	std::optional<std::array<double, 3>> emission; // Ke
	std::optional<double> shininess;               // Ns, unscaled
	std::optional<double> transparency;            // 1 - d, or Tr as written
	std::string map_kd;                            // map_Kd, "" = none
	std::map<std::string, std::string> other;      // every other directive, verbatim
};

struct MtlDocument {
	std::vector<MtlMaterial> materials;
};

//! Parses a Wavefront `.mtl`. Every real is read with `strtod`, so a decimal literal
//! becomes the correctly-rounded double. Unparseable and unnamed directives are skipped;
//! nothing here throws.
MtlDocument ParseMtl(std::string_view text);

//! A `.mtl` named by `mtllib`: its text, or nullopt when it cannot be read.
using MtlLoader = std::function<std::optional<std::string>(const std::string &mtl_name)>;

struct ObjDocument {
	std::vector<std::array<double, 3>> vertices;
	std::vector<std::array<double, 2>> texcoords;
	std::vector<ObjObject> objects;
	std::vector<std::string> mtllibs;  // every name a `mtllib` gave, in order, once each
	MtlDocument materials;             // accumulated over every loaded `mtllib`
	std::vector<std::string> warnings; // survivable problems, one per line, for the caller to log
};

//! Parses a Wavefront `.obj`. Faces with no `o` in force land in one object named
//! `default_object_name`. `load_mtl` reads the files `mtllib` names, so the caller owns
//! path resolution and I/O. Throws CityJSONError (without context) on a malformed
//! vertex line or an unresolvable face index.
ObjDocument ParseObj(std::string_view text, std::string_view default_object_name, const MtlLoader &load_mtl);

} // namespace cityjson
} // namespace duckdb

#pragma once

#include "cityjson/appearance_source.hpp"
#include "cityjson/cityjson_types.hpp"
#include "cityjson/face_triangulation.hpp"
#include "cityjson/json_utils.hpp"

#include <array>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! One polygon of an object: rings (outer first) as indices into the object's vertex
//! pool, optional per-ring UVs (same lengths as the rings), and what it is dressed in.
struct MeshFace {
	std::vector<std::vector<uint32_t>> rings;
	std::vector<std::vector<std::array<double, 2>>> uvs; // empty, or one list per ring
	int32_t surface = -1;                                // index into MeshObject::surfaces
	int64_t material = -1;                               // AppearanceSource material id
	int64_t texture = -1;                                // AppearanceSource texture id
};

struct MeshObject {
	std::string id;
	std::string object_type;
	std::string lod;
	json attributes; // object or null
	std::vector<Vertex3> vertices;
	std::vector<std::string> surfaces; // semantic surface types, in first-use order
	std::vector<MeshFace> faces;
};

struct MeshModel {
	std::vector<MeshObject> objects;
	std::optional<GeographicalExtent> extent; // over every vertex, world coordinates
	Vertex3 origin {0.0, 0.0, 0.0};           // subtracted by the writers
	std::optional<std::string> crs;
	std::vector<std::string> warnings;
};

struct MeshBuildOptions {
	std::optional<std::string> lod; // normalised; nullopt = highest per object
	std::string origin = "auto";    // "auto" | "none" | "x,y,z"
};

MeshModel BuildMeshModel(const std::map<std::string, std::vector<std::pair<std::string, json>>> &objects,
                         const std::vector<std::string> &feature_order, const AppearanceSource &appearance,
                         const MeshBuildOptions &options, const std::optional<std::string> &crs);

std::array<double, 3> DefaultColour(const std::string &surface_type, const std::string &object_type);

std::string FaceGroupName(const MeshObject &object, const MeshFace &face, const AppearanceSource &appearance);

//! Parse "x,y,z"; nullopt for anything else.
std::optional<Vertex3> ParseOriginOption(const std::string &text);

} // namespace cityjson
} // namespace duckdb

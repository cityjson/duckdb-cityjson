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
	std::vector<std::string> surfaces; // semantic surface types, in the source's `semantics.surfaces` order
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

//! What a face is dressed in, as an *identity* rather than as the name it renders to.
//! Two distinct identities can share a name -- a CityJSON material called `RoofSurface`
//! and a default-coloured RoofSurface, or two materials both called `brick` -- and a
//! writer that keyed its material table by the name would collapse them onto whichever
//! it saw first and silently paint one in the other's colour.
enum class MaterialKind { Material, DefaultSurface, DefaultClass };

struct MaterialIdentity {
	MaterialKind kind = MaterialKind::DefaultClass;
	int64_t material_id = -1; //!< MaterialKind::Material
	std::string label;        //!< surface type (DefaultSurface) or object class (DefaultClass)
	int64_t texture_id = -1;  //!< -1 = untextured

	bool operator<(const MaterialIdentity &other) const;
};

//! The identity of `face` within `object`. `texture_id` is what the caller resolved the
//! face's texture to and is passed in rather than read off the face, because a texture
//! whose image could not be had leaves the face with its colour alone -- exactly what an
//! untextured face gets -- and so must not key an entry of its own: the caller passes -1.
MaterialIdentity IdentityOf(const MeshObject &object, const MeshFace &face, const AppearanceSource &appearance,
                            int64_t texture_id);

std::string FaceGroupName(const MeshObject &object, const MeshFace &face, const AppearanceSource &appearance);

//! Parse "x,y,z"; nullopt for anything else.
std::optional<Vertex3> ParseOriginOption(const std::string &text);

} // namespace cityjson
} // namespace duckdb

#pragma once

#include "cityjson/reader.hpp"

#include <array>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
class ClientContext;
}

namespace duckdb {
namespace cityjson {

//! What an OBJ cannot tell us and the caller must: the level of detail the one geometry
//! column is named after, the CityGML class every object gets, how faces become a
//! geometry, and -- for obj_metadata only -- the CRS.
struct OBJReadOptions {
	std::string lod; // normalised, e.g. "2.2"
	std::string object_type = "Building";
	std::string geometry_type = "auto"; // "auto" | "Solid" | "MultiSurface"
	std::optional<std::string> crs;     // surfaces as metadata.referenceSystem (OGC URL form)
};

/**
 * Wavefront OBJ reader.
 *
 * One `o` group is one city object with one geometry at `options.lod`; `usemtl` / `g`
 * names that are CityJSON semantic-surface types become the face's surface; the `.mtl`
 * materials become the appearance definitions, textured ones become textures, and the
 * `vt` list is the UV pool. Everything is parsed once, lazily, and served from a cache.
 * All bytes come through DuckDB's FileSystem, so remote paths work.
 */
class OBJReader : public CityJSONReader {
public:
	OBJReader(ClientContext &context, std::string file_path, OBJReadOptions options);
	~OBJReader() override;

	std::string Name() const override;
	CityJSON ReadMetadata() const override;
	CityJSONFeatureChunk ReadAllChunks() const override;
	std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
	std::vector<Column> Columns() const override;
	size_t CountCityObjects() const override;
	size_t CountFeatures() const override;

	//! True for the CityJSON 2.0 semantic-surface vocabulary (RoofSurface, WallSurface,
	//! GroundSurface, ClosureSurface, OuterCeilingSurface, OuterFloorSurface, Window,
	//! Door, InteriorWallSurface, CeilingSurface, FloorSurface, WaterSurface,
	//! WaterGroundSurface, TrafficArea, AuxiliaryTrafficArea, TransportationMarking,
	//! TransportationHole) and for any `+`-prefixed extension name.
	static bool IsSemanticSurfaceType(const std::string &name);

	//! The `# origin x y z` comment the OBJ writer emits; nullopt when absent.
	static std::optional<std::array<double, 3>> ParseOriginComment(const std::string &content);

	//! Directory part of a path or URL ("a/b/c.obj" -> "a/b", "c.obj" -> "").
	static std::string DirectoryOf(const std::string &path);

private:
	struct Parsed;
	const Parsed &Load() const;

	ClientContext &context_;
	std::string file_path_;
	OBJReadOptions options_;
	mutable std::shared_ptr<const Parsed> parsed_;
	mutable std::optional<std::vector<Column>> cached_columns_;
};

} // namespace cityjson
} // namespace duckdb

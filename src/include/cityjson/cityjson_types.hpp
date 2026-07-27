#pragma once

#include "cityjson/types.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb.hpp"
#include <string>
#include <vector>
#include <map>
#include <optional>
#include <array>
#include <span>

namespace duckdb {
namespace cityjson {

/**
 * Transform parameters for vertex coordinates
 * Vertices are compressed using: real_coord = vertex * scale + translate
 */
struct Transform {
	std::array<double, 3> scale;     // Scale factors for [x, y, z]
	std::array<double, 3> translate; // Translation offsets for [x, y, z]

	Transform();
	Transform(std::array<double, 3> scale, std::array<double, 3> translate);

	/**
	 * Apply transform to compressed vertex coordinates
	 */
	std::array<double, 3> Apply(const std::array<double, 3> &vertex) const;

	/**
	 * Parse transform from JSON object
	 */
	static Transform FromJson(const json &obj);
};

/**
 * Coordinate Reference System definition
 */
struct CRS {
	std::string name;                     // CRS name (e.g., "EPSG:4326")
	std::optional<std::string> authority; // CRS authority (e.g., "EPSG")
	std::optional<std::string> code;      // CRS code (e.g., "4326")

	CRS() = default;
	explicit CRS(std::string name);
	CRS(std::string name, std::string authority, std::string code);

	/**
	 * Parse CRS from JSON object or EPSG code
	 */
	static CRS FromJson(const json &obj);
};

/**
 * CityJSON point of contact (per CityJSON 2.0.1 spec section 5.3)
 * Required: contactName, emailAddress
 * Optional: role, website, contactType, address, phone, organization
 */
struct PointOfContact {
	std::string contact_name;                // Required: name of the contact
	std::string email_address;               // Required: email address
	std::optional<std::string> role;         // Optional: role of the contact
	std::optional<std::string> website;      // Optional: URL of point of contact
	std::optional<std::string> contact_type; // Optional: "individual" or "organization"
	std::optional<json> address;             // Optional: address object
	std::optional<std::string> phone;        // Optional: phone number
	std::optional<std::string> organization; // Optional: organization name

	PointOfContact() = default;
	PointOfContact(std::string contact_name, std::string email_address);

	/**
	 * Parse PointOfContact from JSON object
	 */
	static PointOfContact FromJson(const json &obj);

	/**
	 * Convert to JSON object
	 */
	json ToJson() const;
};

/**
 * Geographical extent (3D bounding box)
 */
struct GeographicalExtent {
	double min_x;
	double min_y;
	double min_z;
	double max_x;
	double max_y;
	double max_z;

	GeographicalExtent() = default;
	GeographicalExtent(double min_x, double min_y, double min_z, double max_x, double max_y, double max_z);

	/**
	 * Parse geographical extent from JSON array [minx, miny, minz, maxx, maxy, maxz]
	 */
	static GeographicalExtent FromJson(const json &arr);

	/**
	 * Convert to JSON array
	 */
	json ToJson() const;
};

/**
 * CityJSON metadata fields
 */
struct Metadata {
	std::optional<std::string> title;
	std::optional<std::string> identifier;
	std::optional<PointOfContact> point_of_contact; // Changed from string to PointOfContact object
	std::optional<std::string> reference_date;
	std::optional<std::string> reference_system;
	std::optional<std::string> geographic_location;
	std::optional<GeographicalExtent> geographical_extent;
	std::optional<std::string> dataset_topic_category;
	std::optional<std::string> feature_type;
	std::optional<std::string> metadata_standard;
	std::optional<std::string> metadata_language;
	std::optional<std::string> metadata_character_set;
	std::optional<std::string> metadata_date;

	/**
	 * Parse metadata from JSON object
	 */
	static Metadata FromJson(const json &obj);
};

/**
 * CityJSON geometry object
 */
struct Geometry {
	std::string type;              // Geometry type (e.g., "Solid", "MultiSurface")
	std::string lod;               // Level of Detail (e.g., "2.1", "1.0")
	json boundaries;               // Geometry boundaries (nested arrays)
	std::optional<json> semantics; // Surface semantics (optional)
	std::optional<json> material;  // Material information (optional)
	std::optional<json> texture;   // Texture information (optional)

	Geometry() = default;
	Geometry(std::string type, std::string lod, json boundaries);

	/**
	 * Parse geometry from JSON object
	 */
	static Geometry FromJson(const json &obj);

	/**
	 * Convert to JSON object
	 */
	json ToJson() const;
};

/**
 * CityJSON CityObject
 * Represents a single city object (building, road, etc.)
 */
struct CityObject {
	std::string type;                                       // CityObject type (e.g., "Building", "Road")
	std::map<std::string, json> attributes;                 // Custom attributes
	std::vector<Geometry> geometry;                         // List of geometries at various LODs
	std::optional<GeographicalExtent> geographical_extent;  // 3D bounding box
	std::vector<std::string> children;                      // Child CityObject IDs
	std::vector<std::string> parents;                       // Parent CityObject IDs
	std::optional<std::vector<std::string>> children_roles; // Roles of children

	CityObject() = default;
	explicit CityObject(std::string type);

	/**
	 * Parse CityObject from JSON object
	 */
	static CityObject FromJson(const json &obj);

	/**
	 * Convert to JSON object
	 */
	json ToJson() const;

	/**
	 * Get geometry at specific LOD
	 */
	std::optional<Geometry> GetGeometryAtLOD(const std::string &lod) const;

	/**
	 * Get the geometry with the highest LOD (by numeric value).
	 * Used to compute the per-row bbox in default (wide) mode. Geometries with an
	 * empty LOD are skipped; if none have a usable LOD the first geometry is returned.
	 */
	std::optional<Geometry> GetHighestLODGeometry() const;
};

/**
 * CityJSON extension definition
 */
struct Extension {
	std::string url;                      // Extension schema URL
	std::string version;                  // Extension version
	std::optional<json> extra_properties; // Additional extension properties

	Extension() = default;
	Extension(std::string url, std::string version);

	/**
	 * Parse extension from JSON object
	 */
	static Extension FromJson(const json &obj);
};

/**
 * A material definition from the CityJSON `appearance.materials` array.
 *
 * One row of the CityParquet `materials.parquet` sidecar. The array position is the
 * dataset-global `id` a geometry's material map references.
 */
struct Material {
	std::optional<std::string> name;
	std::optional<double> ambient_intensity;
	std::optional<std::vector<double>> diffuse_color;  // 3 values in [0,1]
	std::optional<std::vector<double>> specular_color; // 3 values in [0,1]
	std::optional<std::vector<double>> emissive_color; // 3 values in [0,1]
	std::optional<double> transparency;
	std::optional<double> shininess;
	std::optional<bool> is_smooth;
	json other; // members this mapping does not cover

	static Material FromJson(const json &obj);
};

/**
 * A texture definition from the CityJSON `appearance.textures` array.
 *
 * One row of the CityParquet `textures.parquet` sidecar. Two members are renamed on the
 * way in: CityJSON's `image` is the spec's `image_uri`, and CityJSON's `type` is
 * `image_type` (a format token such as "PNG", deliberately not a MIME type). The spec's
 * optional `image_data` has no CityJSON source and is always null here.
 */
struct Texture {
	std::optional<std::string> image_uri;
	std::optional<std::string> image_type;
	std::optional<std::string> wrap_mode;
	std::optional<std::string> texture_type;
	std::optional<std::vector<double>> border_color; // 4 values in [0,1] (RGBA)
	json other;

	static Texture FromJson(const json &obj);
};

/**
 * The CityJSON `appearance` object.
 *
 * Material and texture *definitions* are dataset-global: in CityJSON they sit in the one
 * top-level appearance object, and in CityJSONSeq they sit in the header line. The UV
 * pool is not — each CityJSONSeq feature carries its own `vertices-texture`. That
 * asymmetry is why the CityParquet encoding inlines UV coordinates: a stored UV index
 * would be meaningless once features are merged into a single table.
 */
/**
 * The CityJSON `geometry-templates` object: reusable geometries plus the vertex pool
 * they index.
 *
 * Template geometry is in **local** coordinates and is exempt from the dataset
 * transform — an instance's own transformationMatrix and reference point place it into
 * the world — so `vertices` here are raw doubles, not quantised integers.
 */
struct GeometryTemplates {
	std::vector<Geometry> templates;
	std::vector<std::array<double, 3>> vertices;

	bool Empty() const {
		return templates.empty();
	}

	static GeometryTemplates FromJson(const json &obj);
};

struct Appearance {
	std::vector<Material> materials;
	std::vector<Texture> textures;
	std::vector<std::array<double, 2>> vertices_texture;

	bool Empty() const {
		return materials.empty() && textures.empty() && vertices_texture.empty();
	}

	static Appearance FromJson(const json &obj);
};

/**
 * CityJSONFeature
 * Represents a feature in CityJSONSeq format
 */
struct CityJSONFeature {
	std::string id;                                 // Feature ID
	std::string type;                               // Always "CityJSONFeature"
	std::map<std::string, CityObject> city_objects; // CityObjects in this feature
	// Per-feature local vertex pool (CityJSONSeq): geometry boundary indices reference this
	std::vector<std::array<double, 3>> vertices;
	// Per-feature appearance. In CityJSONSeq the material/texture *definitions* live in
	// the header, but each feature carries its own `vertices-texture` UV pool, which the
	// feature's own texture maps index into.
	std::optional<Appearance> appearance;

	CityJSONFeature() : type("CityJSONFeature") {
	}
	explicit CityJSONFeature(std::string id) : id(std::move(id)), type("CityJSONFeature") {
	}

	/**
	 * Parse CityJSONFeature from JSON object
	 */
	static CityJSONFeature FromJson(const json &obj);

	/**
	 * Convert to JSON object
	 */
	json ToJson() const;

	/**
	 * Count total CityObjects in feature
	 */
	size_t CityObjectCount() const {
		return city_objects.size();
	}
};

/**
 * Main CityJSON container
 */
struct CityJSON {
	std::string version;                                        // CityJSON version (e.g., "2.0")
	std::optional<Transform> transform;                         // Transform for vertex compression
	std::optional<CRS> crs;                                     // Coordinate reference system
	std::optional<Metadata> metadata;                           // Dataset metadata
	std::map<std::string, Extension> extensions;                // Active extensions
	std::optional<std::vector<std::array<double, 3>>> vertices; // Shared vertex pool (optional)
	std::optional<Appearance> appearance;                       // Material/texture definitions
	std::optional<GeometryTemplates> geometry_templates;        // Reusable template geometries

	CityJSON() : version("2.0") {
	}

	/**
	 * Parse CityJSON metadata (without CityObjects) from JSON object
	 */
	static CityJSON FromJson(const json &obj);

	/**
	 * Convert to JSON object
	 */
	json ToJson() const;
};

/**
 * Starting position for a single output batch
 */
struct CityJSONScanPosition {
	size_t chunk_idx = 0;          // Chunk containing the first row
	size_t feature_idx = 0;        // Feature inside the chunk
	size_t city_object_offset = 0; // CityObject offset inside the feature
	size_t start_row = 0;          // Global output row index
};

/**
 * Precomputed scan plan mapping output batches to source positions
 */
struct CityJSONScanPlan {
	std::vector<CityJSONScanPosition> batch_starts; // One entry per batch
	size_t total_rows = 0;                          // Total number of output rows

	/**
	 * Get number of batches
	 */
	size_t BatchCount() const {
		return batch_starts.size();
	}
};

/**
 * Container for CityJSON features divided into chunks
 */
struct CityJSONFeatureChunk {
	std::vector<CityJSONFeature> records; // All features
	std::vector<Range> chunks;            // Chunk boundaries (indices into records)

	CityJSONFeatureChunk() = default;

	/**
	 * Get number of chunks
	 */
	size_t ChunkCount() const {
		return chunks.size();
	}

	/**
	 * Build a scan plan that maps each output batch to its starting position
	 */
	CityJSONScanPlan BuildScanPlan(size_t batch_size = STANDARD_VECTOR_SIZE) const;

	/**
	 * Get number of CityObjects in a specific chunk
	 * Returns nullopt if chunk_idx is out of bounds
	 */
	std::optional<size_t> CityObjectCount(size_t chunk_idx) const;

	/**
	 * Get features in a specific chunk
	 * Returns span of features or nullopt if chunk_idx is out of bounds
	 */
	std::optional<std::span<CityJSONFeature>> GetChunk(size_t chunk_idx);
	std::optional<std::span<const CityJSONFeature>> GetChunk(size_t chunk_idx) const;

	/**
	 * Get total CityObject count across all chunks
	 */
	size_t TotalCityObjectCount() const;

	/**
	 * Create chunk from features with specified chunk size
	 */
	static CityJSONFeatureChunk CreateChunks(std::vector<CityJSONFeature> features, size_t chunk_size);
};

} // namespace cityjson
} // namespace duckdb

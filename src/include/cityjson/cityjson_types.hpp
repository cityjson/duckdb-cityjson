#pragma once

#include "cityjson/types.hpp"
#include "cityjson/json_utils.hpp"
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
	CRS(std::string name);
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
 * CityJSON metadata fields
 */
struct Metadata {
	std::optional<std::string> title;
	std::optional<std::string> identifier;
	std::optional<PointOfContact> point_of_contact; // Changed from string to PointOfContact object
	std::optional<std::string> reference_date;
	std::optional<std::string> reference_system;
	std::optional<std::string> geographic_location;
	std::optional<std::string> geographic_extent;
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
 * CityJSONFeature
 * Represents a feature in CityJSONSeq format
 */
struct CityJSONFeature {
	std::string id;                                 // Feature ID
	std::string type;                               // Always "CityJSONFeature"
	std::map<std::string, CityObject> city_objects; // CityObjects in this feature

	CityJSONFeature() : type("CityJSONFeature") {
	}
	CityJSONFeature(std::string id) : id(std::move(id)), type("CityJSONFeature") {
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

#pragma once

#include "cityjson/reader.hpp"
#include <pugixml.hpp>
#include <unordered_map>

namespace duckdb {
namespace cityjson {

/**
 * Reader for CityGML 2.0 format (.gml, .citygml)
 * Parses XML/GML and maps to the CityJSON internal data model.
 * Each top-level CityObject becomes a CityJSONFeature with its own vertex pool.
 */
class LocalCityGMLReader : public CityJSONReader {
public:
	explicit LocalCityGMLReader(const std::string &file_path, size_t sample_lines = 100);
	LocalCityGMLReader(const std::string &name, std::string content, size_t sample_lines);

	std::string Name() const override;
	CityJSON ReadMetadata() const override;
	CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
	CityJSONFeatureChunk ReadAllChunks() const override;
	std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
	std::vector<Column> Columns() const override;

private:
	std::string file_path_;
	size_t sample_lines_;
	std::optional<std::string> content_;

	mutable std::optional<CityJSON> cached_metadata_;
	mutable std::optional<std::vector<Column>> cached_columns_;
	mutable int id_counter_ = 0; // Synthetic ID counter for objects missing gml:id

	// Load and parse the XML document
	pugi::xml_document LoadXML() const;

	// Extract the local name from a possibly namespaced tag (e.g. "bldg:Building" -> "Building")
	static std::string LocalName(const char *name);

	// Parse CRS from srsName attribute (e.g. "urn:ogc:def:crs:EPSG::7415" or "EPSG:7415")
	static std::optional<CRS> ParseSrsName(const std::string &srs);

	// Vertex deduplication helper: returns index into vertex_pool, inserting if new
	static uint32_t AddVertex(double x, double y, double z, std::vector<std::array<double, 3>> &vertex_pool,
	                          std::unordered_map<std::string, uint32_t> &vertex_index);

	// Coordinate parsing
	static std::vector<std::array<double, 3>> ParsePosList(const std::string &text, int srs_dimension = 3);

	// GML geometry parsing — all return boundary JSON arrays with vertex indices
	json ParseLinearRing(const pugi::xml_node &ring_node, std::vector<std::array<double, 3>> &vertex_pool,
	                     std::unordered_map<std::string, uint32_t> &vertex_index) const;
	json ParsePolygon(const pugi::xml_node &polygon_node, std::vector<std::array<double, 3>> &vertex_pool,
	                  std::unordered_map<std::string, uint32_t> &vertex_index) const;
	json ParseMultiSurface(const pugi::xml_node &ms_node, std::vector<std::array<double, 3>> &vertex_pool,
	                       std::unordered_map<std::string, uint32_t> &vertex_index) const;
	json ParseCompositeSurface(const pugi::xml_node &cs_node, std::vector<std::array<double, 3>> &vertex_pool,
	                           std::unordered_map<std::string, uint32_t> &vertex_index) const;
	json ParseSolid(const pugi::xml_node &solid_node, std::vector<std::array<double, 3>> &vertex_pool,
	                std::unordered_map<std::string, uint32_t> &vertex_index) const;
	json ParseShell(const pugi::xml_node &shell_node, std::vector<std::array<double, 3>> &vertex_pool,
	                std::unordered_map<std::string, uint32_t> &vertex_index) const;
	json ParseMultiSolid(const pugi::xml_node &ms_node, std::vector<std::array<double, 3>> &vertex_pool,
	                     std::unordered_map<std::string, uint32_t> &vertex_index) const;

	// Parse a CityGML geometry property element (e.g. <bldg:lod2Solid>)
	std::optional<Geometry> ParseGeometryProperty(const pugi::xml_node &prop_node, const std::string &lod,
	                                              const std::string &geom_type,
	                                              std::vector<std::array<double, 3>> &vertex_pool,
	                                              std::unordered_map<std::string, uint32_t> &vertex_index) const;

	// Parse LOD tag name (e.g. "lod2Solid" -> {"2", "Solid"})
	static std::optional<std::pair<std::string, std::string>> ParseLODTag(const std::string &local_name);

	// Map CityGML element local names to CityJSON type strings
	static std::optional<std::string> MapCityObjectType(const std::string &local_name);

	// Parse attributes from a CityObject element
	static std::map<std::string, json> ParseAttributes(const pugi::xml_node &obj_node);

	// Parse a top-level CityObject node into a CityJSONFeature
	CityJSONFeature ParseCityObjectNode(const pugi::xml_node &obj_node) const;

	// Find all top-level cityObjectMember nodes
	static std::vector<pugi::xml_node> FindCityObjectMembers(const pugi::xml_node &root);
};

} // namespace cityjson
} // namespace duckdb

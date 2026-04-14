#include "cityjson/citygml_reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include <sstream>
#include <cstdio>
#include <algorithm>
#include <cctype>

namespace duckdb {
namespace cityjson {

// ============================================================
// Constructors
// ============================================================

LocalCityGMLReader::LocalCityGMLReader(const std::string &file_path, size_t sample_lines)
    : file_path_(file_path), sample_lines_(sample_lines) {
}

LocalCityGMLReader::LocalCityGMLReader(const std::string &name, std::string content, size_t sample_lines)
    : file_path_(name), sample_lines_(sample_lines), content_(std::move(content)) {
}

// ============================================================
// Name
// ============================================================

std::string LocalCityGMLReader::Name() const {
	return file_path_;
}

// ============================================================
// Helpers
// ============================================================

std::string LocalCityGMLReader::LocalName(const char *name) {
	std::string s(name);
	auto pos = s.find(':');
	if (pos != std::string::npos) {
		return s.substr(pos + 1);
	}
	return s;
}

pugi::xml_document LocalCityGMLReader::LoadXML() const {
	pugi::xml_document doc;
	pugi::xml_parse_result result;

	if (content_.has_value()) {
		result = doc.load_string(content_.value().c_str());
	} else {
		result = doc.load_file(file_path_.c_str());
	}

	if (!result) {
		throw CityJSONError::Parse("Failed to parse CityGML XML: " + std::string(result.description()), file_path_);
	}

	return doc;
}

std::optional<CRS> LocalCityGMLReader::ParseSrsName(const std::string &srs) {
	if (srs.empty()) {
		return std::nullopt;
	}

	// Handle URN format: "urn:ogc:def:crs:EPSG::7415"
	if (srs.substr(0, 4) == "urn:") {
		auto last_colon = srs.rfind(':');
		if (last_colon != std::string::npos && last_colon + 1 < srs.size()) {
			std::string code = srs.substr(last_colon + 1);
			if (srs.find("EPSG") != std::string::npos) {
				return CRS("EPSG:" + code, "EPSG", code);
			}
		}
	}

	// Handle simple "EPSG:XXXX" format
	if (srs.substr(0, 5) == "EPSG:") {
		std::string code = srs.substr(5);
		return CRS("EPSG:" + code, "EPSG", code);
	}

	// Handle OGC HTTP URI: "http://www.opengis.net/def/crs/EPSG/0/4326"
	auto epsg_pos = srs.find("EPSG");
	if (epsg_pos != std::string::npos) {
		auto last_slash = srs.rfind('/');
		if (last_slash != std::string::npos && last_slash + 1 < srs.size()) {
			std::string code = srs.substr(last_slash + 1);
			return CRS("EPSG:" + code, "EPSG", code);
		}
	}

	// Fall back to storing the full string as the name
	return CRS(srs);
}

uint32_t LocalCityGMLReader::AddVertex(double x, double y, double z,
                                       std::vector<std::array<double, 3>> &vertex_pool,
                                       std::unordered_map<std::string, uint32_t> &vertex_index) {
	char buf[128];
	snprintf(buf, sizeof(buf), "%.15g,%.15g,%.15g", x, y, z);
	std::string key(buf);

	auto it = vertex_index.find(key);
	if (it != vertex_index.end()) {
		return it->second;
	}

	auto idx = static_cast<uint32_t>(vertex_pool.size());
	vertex_pool.push_back({x, y, z});
	vertex_index[key] = idx;
	return idx;
}

std::vector<std::array<double, 3>> LocalCityGMLReader::ParsePosList(const std::string &text, int srs_dimension) {
	std::vector<std::array<double, 3>> coords;
	std::istringstream iss(text);
	double val;
	std::vector<double> values;

	while (iss >> val) {
		values.push_back(val);
	}

	for (size_t i = 0; i + srs_dimension <= values.size(); i += srs_dimension) {
		double x = values[i];
		double y = values[i + 1];
		double z = (srs_dimension >= 3) ? values[i + 2] : 0.0;
		coords.push_back({x, y, z});
	}

	return coords;
}

// ============================================================
// LOD tag parsing
// ============================================================

std::optional<std::pair<std::string, std::string>> LocalCityGMLReader::ParseLODTag(const std::string &local_name) {
	// Match pattern: "lod<digit><GeomType>" e.g. "lod2Solid", "lod1MultiSurface"
	if (local_name.size() < 5 || local_name.substr(0, 3) != "lod") {
		return std::nullopt;
	}

	// Extract LOD digit(s)
	size_t pos = 3;
	while (pos < local_name.size() && std::isdigit(local_name[pos])) {
		pos++;
	}
	if (pos == 3 || pos >= local_name.size()) {
		return std::nullopt;
	}

	std::string lod = local_name.substr(3, pos - 3);
	std::string rest = local_name.substr(pos);

	// Map the rest to a CityJSON geometry type
	// Common CityGML 2.0 geometry property suffixes
	if (rest == "Solid") {
		return std::make_pair(lod, std::string("Solid"));
	}
	if (rest == "MultiSurface") {
		return std::make_pair(lod, std::string("MultiSurface"));
	}
	if (rest == "MultiCurve") {
		return std::make_pair(lod, std::string("MultiLineString"));
	}
	if (rest == "FootPrint" || rest == "RoofEdge") {
		return std::make_pair(lod, std::string("MultiSurface"));
	}
	if (rest == "TerrainIntersection") {
		return std::make_pair(lod, std::string("MultiLineString"));
	}
	if (rest == "MultiPoint") {
		return std::make_pair(lod, std::string("MultiPoint"));
	}
	if (rest == "Geometry") {
		// Generic geometry — type determined from actual GML content
		return std::make_pair(lod, std::string(""));
	}

	return std::nullopt;
}

// ============================================================
// CityObject type mapping
// ============================================================

std::optional<std::string> LocalCityGMLReader::MapCityObjectType(const std::string &local_name) {
	// CityGML 2.0 -> CityJSON type mapping
	// Buildings
	if (local_name == "Building") return std::string("Building");
	if (local_name == "BuildingPart") return std::string("BuildingPart");
	if (local_name == "BuildingInstallation") return std::string("BuildingInstallation");
	if (local_name == "BuildingFurniture") return std::string("BuildingFurniture");
	if (local_name == "Room") return std::string("BuildingRoom");
	if (local_name == "IntBuildingInstallation") return std::string("BuildingInstallation");
	// Transportation
	if (local_name == "Road") return std::string("Road");
	if (local_name == "Railway") return std::string("Railway");
	if (local_name == "Track") return std::string("Road");
	if (local_name == "Square") return std::string("TransportSquare");
	if (local_name == "TransportationComplex") return std::string("Road");
	// Water
	if (local_name == "WaterBody") return std::string("WaterBody");
	// Vegetation
	if (local_name == "PlantCover") return std::string("PlantCover");
	if (local_name == "SolitaryVegetationObject") return std::string("SolitaryVegetationObject");
	// Land use
	if (local_name == "LandUse") return std::string("LandUse");
	// City furniture
	if (local_name == "CityFurniture") return std::string("CityFurniture");
	// Relief
	if (local_name == "ReliefFeature") return std::string("TINRelief");
	if (local_name == "TINRelief") return std::string("TINRelief");
	// Bridge
	if (local_name == "Bridge") return std::string("Bridge");
	if (local_name == "BridgePart") return std::string("BridgePart");
	if (local_name == "BridgeInstallation") return std::string("BridgeInstallation");
	if (local_name == "BridgeConstructionElement") return std::string("BridgeConstructiveElement");
	// Tunnel
	if (local_name == "Tunnel") return std::string("Tunnel");
	if (local_name == "TunnelPart") return std::string("TunnelPart");
	if (local_name == "TunnelInstallation") return std::string("TunnelInstallation");
	// Generics
	if (local_name == "GenericCityObject") return std::string("GenericCityObject");
	// CityObjectGroup
	if (local_name == "CityObjectGroup") return std::string("CityObjectGroup");

	return std::nullopt;
}

// ============================================================
// GML Geometry Parsing
// ============================================================

json LocalCityGMLReader::ParseLinearRing(const pugi::xml_node &ring_node,
                                         std::vector<std::array<double, 3>> &vertex_pool,
                                         std::unordered_map<std::string, uint32_t> &vertex_index) const {
	json indices = json::array();

	// Look for <gml:posList> child
	for (auto child : ring_node.children()) {
		std::string ln = LocalName(child.name());
		if (ln == "posList") {
			int dim = child.attribute("srsDimension").as_int(3);
			auto coords = ParsePosList(child.child_value(), dim);
			for (const auto &c : coords) {
				indices.push_back(AddVertex(c[0], c[1], c[2], vertex_pool, vertex_index));
			}
			break;
		}
		if (ln == "pos") {
			auto coords = ParsePosList(child.child_value(), 3);
			if (!coords.empty()) {
				indices.push_back(AddVertex(coords[0][0], coords[0][1], coords[0][2], vertex_pool, vertex_index));
			}
		}
		if (ln == "coordinates") {
			// Comma-separated format: "x1,y1,z1 x2,y2,z2 ..."
			std::string text = child.child_value();
			std::replace(text.begin(), text.end(), ',', ' ');
			auto coords = ParsePosList(text, 3);
			for (const auto &c : coords) {
				indices.push_back(AddVertex(c[0], c[1], c[2], vertex_pool, vertex_index));
			}
			break;
		}
	}

	// Remove closing vertex if it duplicates the first (CityJSON convention: rings are not closed)
	if (indices.size() > 1 && indices.front() == indices.back()) {
		indices.erase(indices.end() - 1);
	}

	return indices;
}

json LocalCityGMLReader::ParsePolygon(const pugi::xml_node &polygon_node,
                                      std::vector<std::array<double, 3>> &vertex_pool,
                                      std::unordered_map<std::string, uint32_t> &vertex_index) const {
	// CityJSON polygon boundaries = [[exterior_ring], [hole1], [hole2], ...]
	// Exterior ring MUST come first — collect separately to guarantee ordering
	json exterior_ring;
	json interior_rings = json::array();

	for (auto child : polygon_node.children()) {
		std::string ln = LocalName(child.name());
		if (ln != "exterior" && ln != "interior") {
			continue;
		}
		for (auto ring_child : child.children()) {
			if (LocalName(ring_child.name()) == "LinearRing") {
				if (ln == "exterior") {
					exterior_ring = ParseLinearRing(ring_child, vertex_pool, vertex_index);
				} else {
					interior_rings.push_back(ParseLinearRing(ring_child, vertex_pool, vertex_index));
				}
				break;
			}
		}
	}

	json rings = json::array();
	if (!exterior_ring.is_null()) {
		rings.push_back(std::move(exterior_ring));
		for (auto &r : interior_rings) {
			rings.push_back(std::move(r));
		}
	}
	return rings;
}

json LocalCityGMLReader::ParseShell(const pugi::xml_node &shell_node,
                                    std::vector<std::array<double, 3>> &vertex_pool,
                                    std::unordered_map<std::string, uint32_t> &vertex_index) const {
	// A shell = array of polygons (surfaces)
	json surfaces = json::array();

	for (auto child : shell_node.children()) {
		std::string ln = LocalName(child.name());
		if (ln == "surfaceMember") {
			// Find the Polygon inside
			for (auto poly_child : child.children()) {
				std::string pln = LocalName(poly_child.name());
				if (pln == "Polygon") {
					surfaces.push_back(ParsePolygon(poly_child, vertex_pool, vertex_index));
				} else if (pln == "CompositeSurface") {
					// Flatten composite surface members into the shell
					auto cs = ParseCompositeSurface(poly_child, vertex_pool, vertex_index);
					for (auto &surf : cs) {
						surfaces.push_back(std::move(surf));
					}
				}
			}
		}
	}

	return surfaces;
}

json LocalCityGMLReader::ParseSolid(const pugi::xml_node &solid_node,
                                    std::vector<std::array<double, 3>> &vertex_pool,
                                    std::unordered_map<std::string, uint32_t> &vertex_index) const {
	// CityJSON Solid boundaries = [exterior_shell, interior_shell1, ...]
	// Each shell = array of polygon surfaces
	json shells = json::array();

	for (auto child : solid_node.children()) {
		std::string ln = LocalName(child.name());
		if (ln == "exterior" || ln == "interior") {
			// Find Shell/CompositeSurface inside
			for (auto shell_child : child.children()) {
				std::string sln = LocalName(shell_child.name());
				if (sln == "Shell" || sln == "CompositeSurface") {
					shells.push_back(ParseShell(shell_child, vertex_pool, vertex_index));
				}
			}
		}
	}

	return shells;
}

json LocalCityGMLReader::ParseMultiSurface(const pugi::xml_node &ms_node,
                                           std::vector<std::array<double, 3>> &vertex_pool,
                                           std::unordered_map<std::string, uint32_t> &vertex_index) const {
	// CityJSON MultiSurface boundaries = [polygon1, polygon2, ...]
	json surfaces = json::array();

	for (auto child : ms_node.children()) {
		std::string ln = LocalName(child.name());
		if (ln == "surfaceMember") {
			for (auto poly_child : child.children()) {
				std::string pln = LocalName(poly_child.name());
				if (pln == "Polygon") {
					surfaces.push_back(ParsePolygon(poly_child, vertex_pool, vertex_index));
				} else if (pln == "OrientableSurface") {
					// Look for baseSurface -> Polygon
					for (auto base : poly_child.children()) {
						if (LocalName(base.name()) == "baseSurface") {
							for (auto bp : base.children()) {
								if (LocalName(bp.name()) == "Polygon") {
									surfaces.push_back(ParsePolygon(bp, vertex_pool, vertex_index));
								}
							}
						}
					}
				}
			}
		}
	}

	return surfaces;
}

json LocalCityGMLReader::ParseCompositeSurface(const pugi::xml_node &cs_node,
                                               std::vector<std::array<double, 3>> &vertex_pool,
                                               std::unordered_map<std::string, uint32_t> &vertex_index) const {
	// Same structure as MultiSurface for CityJSON purposes
	return ParseMultiSurface(cs_node, vertex_pool, vertex_index);
}

json LocalCityGMLReader::ParseMultiSolid(const pugi::xml_node &ms_node,
                                         std::vector<std::array<double, 3>> &vertex_pool,
                                         std::unordered_map<std::string, uint32_t> &vertex_index) const {
	// CityJSON MultiSolid/CompositeSolid boundaries = [solid1, solid2, ...]
	json solids = json::array();

	for (auto child : ms_node.children()) {
		std::string ln = LocalName(child.name());
		if (ln == "solidMember") {
			for (auto solid_child : child.children()) {
				if (LocalName(solid_child.name()) == "Solid") {
					solids.push_back(ParseSolid(solid_child, vertex_pool, vertex_index));
				}
			}
		}
	}

	return solids;
}

// ============================================================
// Geometry property parsing
// ============================================================

std::optional<Geometry> LocalCityGMLReader::ParseGeometryProperty(
    const pugi::xml_node &prop_node, const std::string &lod, const std::string &geom_type,
    std::vector<std::array<double, 3>> &vertex_pool,
    std::unordered_map<std::string, uint32_t> &vertex_index) const {

	// Find the actual GML geometry element inside the property
	for (auto child : prop_node.children()) {
		std::string ln = LocalName(child.name());
		std::string actual_type = geom_type;

		json boundaries;

		if (ln == "Solid") {
			boundaries = ParseSolid(child, vertex_pool, vertex_index);
			actual_type = "Solid";
		} else if (ln == "MultiSurface") {
			boundaries = ParseMultiSurface(child, vertex_pool, vertex_index);
			actual_type = "MultiSurface";
		} else if (ln == "CompositeSurface") {
			boundaries = ParseCompositeSurface(child, vertex_pool, vertex_index);
			actual_type = "CompositeSurface";
		} else if (ln == "MultiSolid" || ln == "CompositeSolid") {
			boundaries = ParseMultiSolid(child, vertex_pool, vertex_index);
			actual_type = (ln == "MultiSolid") ? "MultiSolid" : "CompositeSolid";
		} else if (ln == "MultiCurve") {
			// MultiCurve -> MultiLineString
			json lines = json::array();
			for (auto member : child.children()) {
				if (LocalName(member.name()) == "curveMember") {
					for (auto curve : member.children()) {
						if (LocalName(curve.name()) == "LineString") {
							json line_indices = json::array();
							for (auto pos_child : curve.children()) {
								std::string pln = LocalName(pos_child.name());
								if (pln == "posList") {
									int dim = pos_child.attribute("srsDimension").as_int(3);
									auto coords = ParsePosList(pos_child.child_value(), dim);
									for (const auto &c : coords) {
										line_indices.push_back(
										    AddVertex(c[0], c[1], c[2], vertex_pool, vertex_index));
									}
								}
							}
							lines.push_back(line_indices);
						}
					}
				}
			}
			boundaries = lines;
			actual_type = "MultiLineString";
		} else if (ln == "Polygon") {
			// Single polygon wrapped as MultiSurface
			boundaries = json::array();
			boundaries.push_back(ParsePolygon(child, vertex_pool, vertex_index));
			actual_type = "MultiSurface";
		} else if (ln == "TriangulatedSurface" || ln == "TIN") {
			// TIN as CompositeSurface
			boundaries = json::array();
			// Look for trianglePatches/Triangle
			for (auto patches : child.children()) {
				std::string patches_ln = LocalName(patches.name());
				if (patches_ln == "trianglePatches" || patches_ln == "patches") {
					for (auto triangle : patches.children()) {
						if (LocalName(triangle.name()) == "Triangle") {
							// Triangle has exterior -> LinearRing
							for (auto ext : triangle.children()) {
								if (LocalName(ext.name()) == "exterior") {
									for (auto ring : ext.children()) {
										if (LocalName(ring.name()) == "LinearRing") {
											json tri_rings = json::array();
											tri_rings.push_back(
											    ParseLinearRing(ring, vertex_pool, vertex_index));
											boundaries.push_back(tri_rings);
										}
									}
								}
							}
						}
					}
				}
			}
			actual_type = "CompositeSurface";
		} else {
			// Unknown geometry type, skip
			continue;
		}

		if (!boundaries.empty()) {
			return Geometry(actual_type, lod, std::move(boundaries));
		}
	}

	return std::nullopt;
}

// ============================================================
// Attribute parsing
// ============================================================

// Known CityGML 2.0 geometry property tag suffixes to skip during attribute extraction
static bool IsGeometryTag(const std::string &local_name) {
	if (local_name.size() >= 4 && local_name.substr(0, 3) == "lod") {
		return true;
	}
	// Other non-attribute elements
	static const std::vector<std::string> skip_tags = {
	    "boundedBy",        "consistsOfBuildingPart",
	    "outerBuildingInstallation", "interiorBuildingInstallation",
	    "interiorRoom",     "opening",
	    "address",          "genericApplicationPropertyOf",
	    "appearanceMember", "appearance",
	    "cityObjectMember", "consistsOf",
	    "trafficArea",      "auxiliaryTrafficArea",
	    "interiorFurniture", "roomInstallation",
	};
	for (const auto &tag : skip_tags) {
		if (local_name == tag) {
			return true;
		}
	}
	return false;
}

std::map<std::string, json> LocalCityGMLReader::ParseAttributes(const pugi::xml_node &obj_node) {
	std::map<std::string, json> attributes;

	for (auto child : obj_node.children()) {
		if (child.type() != pugi::node_element) {
			continue;
		}
		std::string ln = LocalName(child.name());

		// Skip geometry and structural elements
		if (IsGeometryTag(ln)) {
			continue;
		}

		// Skip elements that contain child CityObjects
		if (MapCityObjectType(ln).has_value()) {
			continue;
		}

		// Get text content
		std::string text = child.child_value();
		if (text.empty()) {
			// Check for nested text in child elements (complex attributes)
			// For now, skip complex nested attributes
			continue;
		}

		// Try to infer the value type
		// Try integer
		try {
			size_t pos;
			long long int_val = std::stoll(text, &pos);
			if (pos == text.size()) {
				attributes[ln] = int_val;
				continue;
			}
		} catch (...) {
		}

		// Try double
		try {
			size_t pos;
			double dbl_val = std::stod(text, &pos);
			if (pos == text.size()) {
				attributes[ln] = dbl_val;
				continue;
			}
		} catch (...) {
		}

		// Try boolean
		if (text == "true" || text == "false") {
			attributes[ln] = (text == "true");
			continue;
		}

		// Default: string
		attributes[ln] = text;
	}

	return attributes;
}

// ============================================================
// CityObject node parsing
// ============================================================

CityJSONFeature LocalCityGMLReader::ParseCityObjectNode(const pugi::xml_node &obj_node) const {
	std::string local_name = LocalName(obj_node.name());
	auto type_opt = MapCityObjectType(local_name);
	if (!type_opt.has_value()) {
		throw CityJSONError::InvalidSchema("Unknown CityGML object type: " + local_name, file_path_);
	}

	// Get gml:id
	std::string gml_id = obj_node.attribute("gml:id").as_string("");
	if (gml_id.empty()) {
		gml_id = obj_node.attribute("id").as_string("");
	}
	if (gml_id.empty()) {
		gml_id = "citygml_obj_" + std::to_string(id_counter_++);
	}

	CityJSONFeature feature(gml_id);

	// Per-feature vertex pool
	std::vector<std::array<double, 3>> vertex_pool;
	std::unordered_map<std::string, uint32_t> vertex_index;

	// Create the root CityObject
	CityObject city_object(type_opt.value());
	city_object.attributes = ParseAttributes(obj_node);

	// Parse geometry properties
	for (auto child : obj_node.children()) {
		if (child.type() != pugi::node_element) {
			continue;
		}
		std::string child_ln = LocalName(child.name());
		auto lod_info = ParseLODTag(child_ln);
		if (lod_info.has_value()) {
			auto geom = ParseGeometryProperty(child, lod_info->first, lod_info->second, vertex_pool, vertex_index);
			if (geom.has_value()) {
				city_object.geometry.push_back(std::move(geom.value()));
			}
		}
	}

	// Parse child CityObjects (e.g. BuildingPart inside Building)
	// CityGML 2.0 nests children inside structural elements like <bldg:consistsOfBuildingPart>
	for (auto child : obj_node.children()) {
		if (child.type() != pugi::node_element) {
			continue;
		}
		std::string child_ln = LocalName(child.name());

		// Look for elements that contain nested CityObjects
		for (auto nested : child.children()) {
			if (nested.type() != pugi::node_element) {
				continue;
			}
			std::string nested_ln = LocalName(nested.name());
			auto nested_type = MapCityObjectType(nested_ln);
			if (nested_type.has_value()) {
				// This is a child CityObject
				std::string child_id = nested.attribute("gml:id").as_string("");
				if (child_id.empty()) {
					child_id = nested.attribute("id").as_string("");
				}
				if (child_id.empty()) {
					child_id = gml_id + "_child_" + std::to_string(id_counter_++);
				}

				CityObject child_obj(nested_type.value());
				child_obj.attributes = ParseAttributes(nested);
				child_obj.parents.push_back(gml_id);

				// Parse child geometry (shares the same vertex pool)
				for (auto geom_child : nested.children()) {
					if (geom_child.type() != pugi::node_element) {
						continue;
					}
					std::string gcl = LocalName(geom_child.name());
					auto child_lod_info = ParseLODTag(gcl);
					if (child_lod_info.has_value()) {
						auto geom = ParseGeometryProperty(geom_child, child_lod_info->first,
						                                  child_lod_info->second, vertex_pool, vertex_index);
						if (geom.has_value()) {
							child_obj.geometry.push_back(std::move(geom.value()));
						}
					}
				}

				city_object.children.push_back(child_id);
				feature.city_objects[child_id] = std::move(child_obj);
			}
		}
	}

	feature.city_objects[gml_id] = std::move(city_object);
	feature.vertices = std::move(vertex_pool);

	return feature;
}

// ============================================================
// Find cityObjectMember nodes
// ============================================================

std::vector<pugi::xml_node> LocalCityGMLReader::FindCityObjectMembers(const pugi::xml_node &root) {
	std::vector<pugi::xml_node> members;

	for (auto child : root.children()) {
		std::string ln = LocalName(child.name());
		if (ln == "cityObjectMember" || ln == "featureMember") {
			// The actual CityObject is the first element child
			for (auto obj : child.children()) {
				if (obj.type() == pugi::node_element) {
					members.push_back(obj);
					break;
				}
			}
		}
	}

	return members;
}

// ============================================================
// ReadMetadata
// ============================================================

CityJSON LocalCityGMLReader::ReadMetadata() const {
	if (cached_metadata_.has_value()) {
		return cached_metadata_.value();
	}

	auto doc = LoadXML();
	auto root = doc.first_child();

	CityJSON metadata;
	metadata.version = "CityGML2.0";
	metadata.transform = std::nullopt; // No transform — real-world coordinates

	// Extract srsName from root or Envelope
	std::string srs_name;
	auto root_srs = root.attribute("srsName");
	if (root_srs) {
		srs_name = root_srs.as_string();
	}

	// Look for gml:boundedBy -> gml:Envelope
	for (auto child : root.children()) {
		std::string ln = LocalName(child.name());
		if (ln == "boundedBy") {
			for (auto env_child : child.children()) {
				if (LocalName(env_child.name()) == "Envelope") {
					if (srs_name.empty()) {
						auto env_srs = env_child.attribute("srsName");
						if (env_srs) {
							srs_name = env_srs.as_string();
						}
					}

					// Parse extent
					Metadata meta;
					std::string lower_corner, upper_corner;
					for (auto corner : env_child.children()) {
						std::string cln = LocalName(corner.name());
						if (cln == "lowerCorner") {
							lower_corner = corner.child_value();
						} else if (cln == "upperCorner") {
							upper_corner = corner.child_value();
						}
					}

					if (!lower_corner.empty() && !upper_corner.empty()) {
						auto lc = ParsePosList(lower_corner);
						auto uc = ParsePosList(upper_corner);
						if (!lc.empty() && !uc.empty()) {
							meta.geographic_extent = GeographicalExtent(lc[0][0], lc[0][1], lc[0][2], uc[0][0],
							                                            uc[0][1], uc[0][2]);
						}
					}

					metadata.metadata = meta;
				}
			}
		}

		// Extract gml:name as title
		if (ln == "name") {
			if (!metadata.metadata.has_value()) {
				metadata.metadata = Metadata();
			}
			metadata.metadata->title = child.child_value();
		}
	}

	if (!srs_name.empty()) {
		metadata.crs = ParseSrsName(srs_name);
	}

	cached_metadata_ = metadata;
	return metadata;
}

// ============================================================
// ReadNFeatures
// ============================================================

std::vector<CityJSONFeature> LocalCityGMLReader::ReadNFeatures(size_t n) const {
	auto doc = LoadXML();
	auto root = doc.first_child();
	auto members = FindCityObjectMembers(root);

	std::vector<CityJSONFeature> features;
	size_t count = 0;

	for (const auto &obj_node : members) {
		if (count >= n) {
			break;
		}
		std::string ln = LocalName(obj_node.name());
		if (MapCityObjectType(ln).has_value()) {
			features.push_back(ParseCityObjectNode(obj_node));
			count++;
		}
	}

	return features;
}

// ============================================================
// ReadAllChunks
// ============================================================

CityJSONFeatureChunk LocalCityGMLReader::ReadAllChunks() const {
	auto doc = LoadXML();
	auto root = doc.first_child();
	auto members = FindCityObjectMembers(root);

	std::vector<CityJSONFeature> features;

	for (const auto &obj_node : members) {
		std::string ln = LocalName(obj_node.name());
		if (MapCityObjectType(ln).has_value()) {
			features.push_back(ParseCityObjectNode(obj_node));
		}
	}

	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

// ============================================================
// ReadNthChunk
// ============================================================

CityJSONFeatureChunk LocalCityGMLReader::ReadNthChunk(size_t n) const {
	CityJSONFeatureChunk all_chunks = ReadAllChunks();

	if (n >= all_chunks.ChunkCount()) {
		return CityJSONFeatureChunk();
	}

	auto chunk_opt = all_chunks.GetChunk(n);
	if (!chunk_opt.has_value()) {
		return CityJSONFeatureChunk();
	}

	CityJSONFeatureChunk result;
	result.records = std::vector<CityJSONFeature>(chunk_opt->begin(), chunk_opt->end());
	result.chunks = {Range(0, result.records.size())};
	return result;
}

// ============================================================
// Columns
// ============================================================

std::vector<Column> LocalCityGMLReader::Columns() const {
	if (cached_columns_.has_value()) {
		return cached_columns_.value();
	}

	std::vector<Column> columns = GetDefinedColumns();

	std::vector<CityJSONFeature> sample_features = ReadNFeatures(sample_lines_);

	std::vector<Column> attr_columns = CityObjectUtils::InferAttributeColumns(sample_features, sample_lines_);
	std::vector<Column> geom_columns = CityObjectUtils::InferGeometryColumns(sample_features, sample_lines_);

	columns.insert(columns.end(), attr_columns.begin(), attr_columns.end());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());

	cached_columns_ = columns;
	return columns;
}

} // namespace cityjson
} // namespace duckdb

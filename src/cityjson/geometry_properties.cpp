#include "cityjson/geometry_properties.hpp"
#include "cityjson/error.hpp"
#include <unordered_map>

namespace duckdb {
namespace cityjson {

// =============================================================================
// Type code mappings
// =============================================================================

static const std::unordered_map<std::string, int> CITYJSON_TYPE_CODES = {
    {"Point", 1},   {"MultiPoint", 2},       {"LineString", 3}, {"MultiLineString", 4},
    {"Surface", 5}, {"CompositeSurface", 6}, {"TIN", 7},        {"MultiSurface", 8},
    {"Solid", 9},   {"CompositeSolid", 10},  {"MultiSolid", 11}};

static const std::unordered_map<int, std::string> TYPE_CODE_NAMES = {
    {1, "Point"},   {2, "MultiPoint"},       {3, "LineString"}, {4, "MultiLineString"},
    {5, "Surface"}, {6, "CompositeSurface"}, {7, "TIN"},        {8, "MultiSurface"},
    {9, "Solid"},   {10, "CompositeSolid"},  {11, "MultiSolid"}};

// =============================================================================
// Public methods
// =============================================================================

int GeometryPropertiesSerializer::GetTypeCode(const std::string &cityjson_type) {
	auto it = CITYJSON_TYPE_CODES.find(cityjson_type);
	if (it != CITYJSON_TYPE_CODES.end()) {
		return it->second;
	}
	// Default to -1 for unknown types
	return -1;
}

std::string GeometryPropertiesSerializer::GetTypeName(int type_code) {
	auto it = TYPE_CODE_NAMES.find(type_code);
	if (it != TYPE_CODE_NAMES.end()) {
		return it->second;
	}
	return "Unknown";
}

json GeometryPropertiesSerializer::Serialize(const Geometry &geometry) {
	json result;

	// Type code
	int type_code = GetTypeCode(geometry.type);
	result["type"] = type_code;

	// Original CityJSON type name
	result["cityjsonType"] = geometry.type;

	// Level of Detail
	result["lod"] = geometry.lod;

	// Semantics (if present)
	if (geometry.semantics.has_value()) {
		result["semantics"] = SerializeSemantics(geometry.semantics.value());
	}

	// For complex geometries (Solid, MultiSolid, CompositeSolid),
	// we could build a children hierarchy, but for now we just store
	// the structure info needed for round-trip conversion
	if (type_code >= 9 && type_code <= 11) {
		// Count shells for Solid
		if (type_code == 9 && geometry.boundaries.is_array()) {
			result["shellCount"] = geometry.boundaries.size();
		}
		// Count solids for MultiSolid/CompositeSolid
		if ((type_code == 10 || type_code == 11) && geometry.boundaries.is_array()) {
			result["solidCount"] = geometry.boundaries.size();
		}
	}

	return result;
}

json GeometryPropertiesSerializer::SerializeSemantics(const json &semantics_json) {
	json result;

	// Copy the semantics structure directly
	// CityJSON semantics format:
	// {
	//   "surfaces": [{"type": "WallSurface", ...}, ...],
	//   "values": [0, 1, null, 2, ...]
	// }

	if (semantics_json.contains("surfaces")) {
		result["surfaces"] = json::array();

		for (const auto &surface : semantics_json["surfaces"]) {
			json surf_obj;

			// Required: type
			if (surface.contains("type")) {
				surf_obj["type"] = surface["type"];
			}

			// Optional: other attributes (slope, solar-potential, etc.)
			for (auto it = surface.begin(); it != surface.end(); ++it) {
				if (it.key() != "type" && it.key() != "parent" && it.key() != "children") {
					// Skip structural fields, copy attribute fields
					surf_obj[it.key()] = it.value();
				}
			}

			// Optional: parent reference
			if (surface.contains("parent")) {
				surf_obj["parent"] = surface["parent"];
			}

			// Optional: children references
			if (surface.contains("children")) {
				surf_obj["children"] = surface["children"];
			}

			result["surfaces"].push_back(surf_obj);
		}
	}

	if (semantics_json.contains("values")) {
		result["values"] = semantics_json["values"];
	}

	return result;
}

GeometryProperties GeometryPropertiesSerializer::Build(const Geometry &geometry) {
	GeometryProperties props;

	props.type = GetTypeCode(geometry.type);
	props.cityjson_type = geometry.type;
	props.lod = geometry.lod;

	// Build children for complex geometries
	if (props.type >= 9 && props.type <= 11) {
		props.children = BuildChildren(geometry);
	}

	// Parse semantics if present
	if (geometry.semantics.has_value()) {
		const auto &sem_json = geometry.semantics.value();
		GeometrySemantics semantics;

		if (sem_json.contains("surfaces")) {
			for (const auto &surf : sem_json["surfaces"]) {
				SemanticSurface ss;
				if (surf.contains("type")) {
					ss.type = surf["type"].get<std::string>();
				}
				// Store other attributes
				ss.attributes = surf;
				semantics.surfaces.push_back(std::move(ss));
			}
		}

		if (sem_json.contains("values")) {
			semantics.values = sem_json["values"];
		}

		props.semantics = std::move(semantics);
	}

	return props;
}

// =============================================================================
// Private methods
// =============================================================================

std::vector<GeometryPropertyChild> GeometryPropertiesSerializer::BuildChildren(const Geometry &geometry) {
	std::vector<GeometryPropertyChild> children;

	// For Solid: create children for each shell and surface
	if (geometry.type == "Solid" && geometry.boundaries.is_array()) {
		size_t polygon_index = 0;

		for (size_t shell_idx = 0; shell_idx < geometry.boundaries.size(); ++shell_idx) {
			const auto &shell = geometry.boundaries[shell_idx];

			// Create a child for the shell
			GeometryPropertyChild shell_child;
			shell_child.type = 6; // CompositeSurface
			shell_child.cityjson_type = "CompositeSurface";
			shell_child.object_id = (shell_idx == 0) ? "exteriorShell" : "interiorShell" + std::to_string(shell_idx);
			children.push_back(shell_child);

			// Create children for each surface in the shell
			if (shell.is_array()) {
				for (size_t surf_idx = 0; surf_idx < shell.size(); ++surf_idx) {
					GeometryPropertyChild surf_child;
					surf_child.type = 5; // Surface
					surf_child.cityjson_type = "Surface";
					surf_child.parent = shell_idx;
					surf_child.geometry_index = polygon_index++;
					children.push_back(surf_child);
				}
			}
		}
	}
	// For MultiSolid/CompositeSolid: create children for each solid
	else if ((geometry.type == "MultiSolid" || geometry.type == "CompositeSolid") && geometry.boundaries.is_array()) {
		size_t polygon_index = 0;

		for (size_t solid_idx = 0; solid_idx < geometry.boundaries.size(); ++solid_idx) {
			const auto &solid = geometry.boundaries[solid_idx];

			// Create a child for the solid
			GeometryPropertyChild solid_child;
			solid_child.type = 9; // Solid
			solid_child.cityjson_type = "Solid";
			children.push_back(solid_child);

			// Count polygons in this solid for geometry_index mapping
			if (solid.is_array()) {
				for (const auto &shell : solid) {
					if (shell.is_array()) {
						polygon_index += shell.size();
					}
				}
			}
		}
	}

	return children;
}

size_t GeometryPropertiesSerializer::CountPolygonsInSolid(const json &boundaries) {
	size_t count = 0;

	if (boundaries.is_array()) {
		for (const auto &shell : boundaries) {
			if (shell.is_array()) {
				count += shell.size();
			}
		}
	}

	return count;
}

} // namespace cityjson
} // namespace duckdb

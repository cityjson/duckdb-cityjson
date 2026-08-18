#include "cityjson/city_object_utils.hpp"
#include <set>
#include <algorithm>
#include "cityjson/column_types.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/wkb_encoder.hpp"
#include "cityjson/geometry_properties.hpp"
#include <map>
#include <set>
#include <algorithm>
#include <cctype>
#include <cstring>

namespace duckdb {
namespace cityjson {

// ============================================================
// CityObjectUtils - Attribute Extraction
// ============================================================

json CityObjectUtils::GetAttributeValue(const CityObject &obj, const Column &col) {
	// Handle predefined columns
	if (col.name == "object_type") {
		return json(obj.type);
	}

	if (col.name == "children") {
		if (obj.children.empty()) {
			return json(nullptr);
		}
		return json(obj.children);
	}

	if (col.name == "parents") {
		if (obj.parents.empty()) {
			return json(nullptr);
		}
		return json(obj.parents);
	}

	if (col.name == "children_roles") {
		if (!obj.children_roles.has_value() || obj.children_roles->empty()) {
			return json(nullptr);
		}
		// Preserve null slots positionally -- WriteVarcharArray already writes a
		// non-string array element as a SQL NULL list entry.
		json roles = json::array();
		for (const auto &role : obj.children_roles.value()) {
			roles.push_back(role.has_value() ? json(role.value()) : json(nullptr));
		}
		return roles;
	}

	if (col.name == "geographical_extent") {
		if (!obj.geographical_extent.has_value()) {
			return json(nullptr);
		}
		return obj.geographical_extent->ToJson();
	}

	if (col.name == "other") {
		// Return attributes not in standard columns
		json other_attrs = json::object();
		for (const auto &[key, value] : obj.attributes) {
			if (!IsPredefinedColumn(key) && !IsGeometryColumn(key)) {
				other_attrs[key] = value;
			}
		}
		if (other_attrs.empty()) {
			return json(nullptr);
		}
		return other_attrs;
	}

	// Dynamic attribute column - look up in attributes map
	auto it = obj.attributes.find(col.name);
	if (it != obj.attributes.end()) {
		return it->second;
	}

	// Attribute not found
	return json(nullptr);
}

// ============================================================
// CityObjectUtils - Schema Inference
// ============================================================

std::vector<Column> CityObjectUtils::InferAttributeColumns(const std::vector<CityJSONFeature> &features,
                                                           size_t sample_size) {
	if (features.empty()) {
		return {};
	}

	// Determine how many features to sample
	size_t num_to_sample = std::min(sample_size, features.size());

	// Map of attribute name -> list of observed types
	std::map<std::string, std::vector<ColumnType>> attribute_types;

	// Sample features and collect attribute keys
	for (size_t i = 0; i < num_to_sample; i++) {
		const auto &feature = features[i];

		// Iterate through all CityObjects in the feature
		for (const auto &[city_obj_id, city_obj] : feature.city_objects) {
			// Collect all attributes
			for (const auto &[attr_key, attr_value] : city_obj.attributes) {
				// Reserved columns take precedence over dynamic attributes: an attribute
				// whose name collides (case-insensitively) with a reserved column does not
				// get its own column. Its value is still preserved in the `other` JSON.
				if (IsReservedColumnName(attr_key)) {
					continue;
				}

				// Infer type from value
				ColumnType inferred_type = ColumnTypeUtils::InferFromJson(attr_value);
				attribute_types[attr_key].push_back(inferred_type);
			}
		}
	}

	// Resolve final type for each attribute. Attribute names that differ only by case
	// would also produce duplicate DuckDB columns, so keep only the first (the map is
	// ordered, so this is deterministic).
	std::vector<Column> result;
	std::set<std::string> seen_lower;
	for (const auto &[attr_name, types] : attribute_types) {
		std::string lowered = attr_name;
		std::transform(lowered.begin(), lowered.end(), lowered.begin(),
		               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
		if (!seen_lower.insert(lowered).second) {
			continue;
		}
		ColumnType resolved_type = ColumnTypeUtils::ResolveFromSamples(types);
		result.emplace_back(attr_name, resolved_type);
	}

	// Sort by name for consistent ordering
	std::sort(result.begin(), result.end(), [](const Column &a, const Column &b) { return a.name < b.name; });

	return result;
}

std::vector<Column> CityObjectUtils::InferGeometryColumns(const std::vector<CityJSONFeature> &features,
                                                          size_t sample_size) {
	if (features.empty()) {
		return {};
	}

	// Determine how many features to sample
	size_t num_to_sample = std::min(sample_size, features.size());

	// Set of unique LODs found
	std::set<std::string> lods;

	// Sample features and collect LODs
	for (size_t i = 0; i < num_to_sample; i++) {
		const auto &feature = features[i];

		// Iterate through all CityObjects in the feature
		for (const auto &[city_obj_id, city_obj] : feature.city_objects) {
			// Collect LODs from all geometries
			for (const auto &geom : city_obj.geometry) {
				if (!geom.lod.empty()) {
					lods.insert(LODTableUtils::NormalizeLOD(geom.lod));
				}
			}
		}
	}

	// Create per-LOD WKB geometry columns (CityParquet-style wide layout):
	// for each LOD, a "geometry_lodX_Y" (WKB BLOB) and "geometry_properties_lodX_Y" (JSON).
	// `lods` is a std::set, so iteration is already sorted; emit the pair per LOD so the
	// geometry and its properties stay adjacent.
	std::vector<Column> result;
	for (const auto &lod : lods) {
		std::string suffix = LODTableUtils::FormatLODAsColumnSuffix(lod);
		result.emplace_back("geometry_" + suffix, ColumnType::GeometryWKB);
		result.emplace_back("geometry_properties_" + suffix, ColumnType::GeometryPropertiesStruct);
		// Per-LoD appearance columns paired to the geometry by name (§11.1). Present
		// for every LoD that has a geometry column, whether or not any row carries
		// appearance for it (nullable).
		result.emplace_back("material_" + suffix, ColumnType::AppearanceJson);
		result.emplace_back("texture_" + suffix, ColumnType::AppearanceJson);
	}

	// A single per-row bbox (computed from the highest-LOD geometry) completes the
	// CityParquet wide layout. Only emitted when at least one LOD geometry exists.
	if (!lods.empty()) {
		result.emplace_back("bbox", ColumnType::GeographicalExtent);
	}

	return result;
}

CompactedGeometry CityObjectUtils::GetGeometryArrowNative(const Geometry &geometry,
                                                          const std::vector<std::array<double, 3>> &vertices,
                                                          const std::optional<Transform> &transform) {
	return ArrowNativeEncoder::Encode(geometry, vertices, transform);
}

void CityObjectUtils::ApplyGeometryEncoding(std::vector<Column> &columns, GeometryEncoding encoding) {
	if (encoding == GeometryEncoding::Wkb) {
		return;
	}

	// The vertex-pool column is linked to its geometry column purely by name, the
	// same way geometry_properties_lod*/material_lod*/texture_lod* already are --
	// no metadata field cross-references them (design doc, "File-level provenance").
	std::vector<Column> result;
	result.reserve(columns.size());
	for (auto &column : columns) {
		if (column.kind != ColumnType::GeometryWKB) {
			result.push_back(std::move(column));
			continue;
		}
		static constexpr const char *PREFIX = "geometry_";
		const size_t prefix_len = std::strlen(PREFIX);
		if (column.name.compare(0, prefix_len, PREFIX) != 0) {
			// Both derivations name every WKB geometry column "geometry_<suffix>", and
			// the sibling's name is derived from that suffix. If the convention ever
			// breaks, say so here rather than emit a sibling nothing can pair up.
			throw CityJSONError::Other("arrow-native encoding: geometry column '" + column.name +
			                           "' does not start with '" + PREFIX + "'");
		}
		std::string suffix = column.name.substr(prefix_len);
		result.emplace_back(column.name, ColumnType::GeometryArrowNative);
		result.emplace_back("geometry_vertices_" + suffix, ColumnType::GeometryVerticesArrowNative);
	}
	columns = std::move(result);
}

// ============================================================
// CityObjectUtils - Geometry Encoding
// ============================================================

std::vector<uint8_t> CityObjectUtils::GetGeometryWKB(const Geometry &geometry,
                                                     const std::vector<std::array<double, 3>> &vertices,
                                                     const std::optional<Transform> &transform) {
	return WKBEncoder::Encode(geometry, vertices, transform);
}

json CityObjectUtils::GetGeometryPropertiesStruct(const Geometry &geometry,
                                                  const std::optional<std::string> &object_id) {
	// Note: object_id parameter reserved for future use
	(void)object_id; // Suppress unused parameter warning
	return GeometryPropertiesSerializer::Serialize(geometry);
}

static void CollectExtentRecursive(const json &boundaries, const std::vector<std::array<double, 3>> &vertices,
                                   const std::optional<Transform> &transform, GeographicalExtent &extent, bool &found) {
	if (boundaries.is_number_integer()) {
		auto idx = boundaries.get<int64_t>();
		if (idx < 0 || static_cast<size_t>(idx) >= vertices.size()) {
			return; // skip invalid index
		}
		std::array<double, 3> v = vertices[static_cast<size_t>(idx)];
		if (transform.has_value()) {
			v = transform->Apply(v);
		}
		if (!found) {
			extent.min_x = extent.max_x = v[0];
			extent.min_y = extent.max_y = v[1];
			extent.min_z = extent.max_z = v[2];
			found = true;
		} else {
			extent.min_x = std::min(extent.min_x, v[0]);
			extent.min_y = std::min(extent.min_y, v[1]);
			extent.min_z = std::min(extent.min_z, v[2]);
			extent.max_x = std::max(extent.max_x, v[0]);
			extent.max_y = std::max(extent.max_y, v[1]);
			extent.max_z = std::max(extent.max_z, v[2]);
		}
		return;
	}
	if (boundaries.is_array()) {
		for (const auto &child : boundaries) {
			CollectExtentRecursive(child, vertices, transform, extent, found);
		}
	}
}

namespace {

void MergeExtent(std::optional<GeographicalExtent> &into, const GeographicalExtent &other) {
	if (!into.has_value()) {
		into = other;
		return;
	}
	into->min_x = std::min(into->min_x, other.min_x);
	into->min_y = std::min(into->min_y, other.min_y);
	into->min_z = std::min(into->min_z, other.min_z);
	into->max_x = std::max(into->max_x, other.max_x);
	into->max_y = std::max(into->max_y, other.max_y);
	into->max_z = std::max(into->max_z, other.max_z);
}

void AccumulateObjectExtent(const std::string &object_id, const std::map<std::string, CityObject> &objects,
                            const std::vector<std::array<double, 3>> &vertices,
                            const std::optional<Transform> &transform, std::set<std::string> &visited,
                            std::optional<GeographicalExtent> &result) {
	if (!visited.insert(object_id).second) {
		return; // already folded in; also guards a cyclic hierarchy
	}
	auto entry = objects.find(object_id);
	if (entry == objects.end()) {
		return; // dangling child reference contributes nothing
	}
	const auto &object = entry->second;

	// Every stored LoD, not just the highest.
	for (const auto &geometry : object.geometry) {
		auto extent = CityObjectUtils::GetGeometryExtent(geometry, vertices, transform);
		if (extent.has_value()) {
			MergeExtent(result, extent.value());
		}
	}
	for (const auto &child_id : object.children) {
		AccumulateObjectExtent(child_id, objects, vertices, transform, visited, result);
	}
}

} // namespace

std::optional<GeographicalExtent> CityObjectUtils::GetObjectExtent(const std::string &object_id,
                                                                   const std::map<std::string, CityObject> &objects,
                                                                   const std::vector<std::array<double, 3>> &vertices,
                                                                   const std::optional<Transform> &transform) {
	std::optional<GeographicalExtent> result;
	std::set<std::string> visited;
	AccumulateObjectExtent(object_id, objects, vertices, transform, visited, result);
	return result;
}

std::optional<GeographicalExtent> CityObjectUtils::GetGeometryExtent(const Geometry &geometry,
                                                                     const std::vector<std::array<double, 3>> &vertices,
                                                                     const std::optional<Transform> &transform) {
	GeographicalExtent extent;
	bool found = false;
	CollectExtentRecursive(geometry.boundaries, vertices, transform, extent, found);
	if (!found) {
		return std::nullopt;
	}
	return extent;
}

} // namespace cityjson
} // namespace duckdb

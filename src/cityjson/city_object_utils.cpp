#include "cityjson/city_object_utils.hpp"
#include "cityjson/column_types.hpp"
#include <map>
#include <set>
#include <algorithm>

namespace duckdb {
namespace cityjson {

// ============================================================
// CityObjectUtils - Attribute Extraction
// ============================================================

json CityObjectUtils::GetAttributeValue(const CityObject& obj, const Column& col) {
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
        return json(obj.children_roles.value());
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
        for (const auto& [key, value] : obj.attributes) {
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
// CityObjectUtils - Geometry Extraction
// ============================================================

json CityObjectUtils::GetGeometryValue(const CityObject& obj, const Column& col) {
    // Parse LOD from column name (e.g., "geom_lod2_1" -> "2.1")
    if (!IsGeometryColumn(col.name)) {
        throw CityJSONError::InvalidSchema("Not a geometry column: " + col.name);
    }

    std::string lod = ParseLODFromColumnName(col.name);

    // Find geometry with matching LOD
    auto geom_opt = obj.GetGeometryAtLOD(lod);
    if (!geom_opt.has_value()) {
        return json(nullptr);
    }

    // Return geometry as JSON object
    return geom_opt->ToJson();
}

// ============================================================
// CityObjectUtils - Schema Inference
// ============================================================

std::vector<Column> CityObjectUtils::InferAttributeColumns(
    const std::vector<CityJSONFeature>& features,
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
        const auto& feature = features[i];

        // Iterate through all CityObjects in the feature
        for (const auto& [city_obj_id, city_obj] : feature.city_objects) {
            // Collect all attributes
            for (const auto& [attr_key, attr_value] : city_obj.attributes) {
                // Skip predefined columns
                if (IsPredefinedColumn(attr_key)) {
                    continue;
                }

                // Infer type from value
                ColumnType inferred_type = ColumnTypeUtils::InferFromJson(attr_value);
                attribute_types[attr_key].push_back(inferred_type);
            }
        }
    }

    // Resolve final type for each attribute
    std::vector<Column> result;
    for (const auto& [attr_name, types] : attribute_types) {
        ColumnType resolved_type = ColumnTypeUtils::ResolveFromSamples(types);
        result.emplace_back(attr_name, resolved_type);
    }

    // Sort by name for consistent ordering
    std::sort(result.begin(), result.end(),
              [](const Column& a, const Column& b) { return a.name < b.name; });

    return result;
}

std::vector<Column> CityObjectUtils::InferGeometryColumns(
    const std::vector<CityJSONFeature>& features,
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
        const auto& feature = features[i];

        // Iterate through all CityObjects in the feature
        for (const auto& [city_obj_id, city_obj] : feature.city_objects) {
            // Collect LODs from all geometries
            for (const auto& geom : city_obj.geometry) {
                lods.insert(geom.lod);
            }
        }
    }

    // Create geometry columns for each LOD
    std::vector<Column> result;
    for (const auto& lod : lods) {
        // Convert LOD "2.1" to column name "geom_lod2_1"
        std::string col_name = "geom_lod";

        // Replace '.' with '_'
        for (char c : lod) {
            if (c == '.') {
                col_name += '_';
            } else {
                col_name += c;
            }
        }

        result.emplace_back(col_name, ColumnType::Geometry);
    }

    // Sort by LOD for consistent ordering
    std::sort(result.begin(), result.end(),
              [](const Column& a, const Column& b) { return a.name < b.name; });

    return result;
}

} // namespace cityjson
} // namespace duckdb

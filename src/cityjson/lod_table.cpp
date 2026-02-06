#include "cityjson/lod_table.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/column_types.hpp"
#include <algorithm>
#include <regex>

namespace duckdb {
namespace cityjson {

// =============================================================================
// Table Name Generation
// =============================================================================

std::string LODTableUtils::GetTableNameForLOD(const std::string &lod, const std::string &base_name) {
	// Convert "2.2" to "lod2_2"
	std::string lod_suffix = FormatLODAsColumnSuffix(lod);
	return base_name + "_" + lod_suffix;
}

std::string LODTableUtils::FormatLODAsColumnSuffix(const std::string &lod) {
	// Convert "2.2" to "lod2_2"
	std::string result = "lod";

	for (char c : lod) {
		if (c == '.') {
			result += '_';
		} else {
			result += c;
		}
	}

	return result;
}

std::string LODTableUtils::ParseLODFromSuffix(const std::string &column_suffix) {
	// Convert "lod2_2" to "2.2"
	std::regex lod_pattern(R"(lod(\d+)_(\d+))");
	std::smatch match;

	if (std::regex_match(column_suffix, match, lod_pattern)) {
		return match[1].str() + "." + match[2].str();
	}

	// Try simple format "2_2"
	std::regex simple_pattern(R"((\d+)_(\d+))");
	if (std::regex_match(column_suffix, match, simple_pattern)) {
		return match[1].str() + "." + match[2].str();
	}

	return column_suffix; // Return as-is if no match
}

// =============================================================================
// Column Definitions
// =============================================================================

std::vector<Column> LODTableUtils::GetBaseColumns() {
	return {
	    Column("id", ColumnType::Varchar),           Column("feature_id", ColumnType::Varchar),
	    Column("object_type", ColumnType::Varchar),  Column("children", ColumnType::VarcharArray),
	    Column("parents", ColumnType::VarcharArray),
	};
}

std::vector<Column> LODTableUtils::GetGeometryColumns() {
	return {
	    Column("geometry", ColumnType::GeometryWKB),
	    Column("geometry_properties", ColumnType::GeometryPropertiesJson),
	};
}

// =============================================================================
// LOD Collection and Table Inference
// =============================================================================

std::set<std::string> LODTableUtils::CollectLODs(const std::vector<CityJSONFeature> &features, size_t sample_size) {
	std::set<std::string> lods;

	// Sample features
	size_t count = std::min(sample_size, features.size());

	for (size_t i = 0; i < count; ++i) {
		const auto &feature = features[i];

		// Iterate through all city objects in the feature
		for (const auto &[obj_id, obj] : feature.city_objects) {
			// Collect LODs from geometries
			for (const auto &geom : obj.geometry) {
				if (!geom.lod.empty()) {
					lods.insert(geom.lod);
				}
			}
		}
	}

	return lods;
}

std::vector<LODTableDefinition> LODTableUtils::InferLODTables(const std::vector<CityJSONFeature> &features,
                                                              size_t sample_size) {
	std::vector<LODTableDefinition> tables;

	// Collect all unique LODs
	auto lods = CollectLODs(features, sample_size);

	if (lods.empty()) {
		// No LODs found - return empty
		return tables;
	}

	// Infer attribute columns (same for all LOD tables)
	auto attribute_columns = CityObjectUtils::InferAttributeColumns(features, sample_size);

	// Create table definition for each LOD
	for (const auto &lod : lods) {
		LODTableDefinition table;
		table.lod_value = lod;
		table.table_name = GetTableNameForLOD(lod);

		// Add base columns
		auto base_cols = GetBaseColumns();
		table.columns.insert(table.columns.end(), base_cols.begin(), base_cols.end());

		// Add attribute columns
		table.columns.insert(table.columns.end(), attribute_columns.begin(), attribute_columns.end());

		// Add geometry columns
		auto geom_cols = GetGeometryColumns();
		table.columns.insert(table.columns.end(), geom_cols.begin(), geom_cols.end());

		tables.push_back(std::move(table));
	}

	return tables;
}

} // namespace cityjson
} // namespace duckdb

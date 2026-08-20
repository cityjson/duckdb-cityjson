#include "cityjson/lod_table.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/column_types.hpp"
#include <algorithm>
#include <regex>
#include <sstream>
#include <iomanip>

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

// Trims trailing zeros from a fixed-precision decimal string, but always keeps
// exactly one digit after the decimal point: "2.000000000000" -> "2.0", never
// "2" (spec §9 — a column suffix always carries a minor).
static std::string TrimTrailingZeros(const std::string &s) {
	size_t end = s.find_last_not_of('0');
	if (end == std::string::npos) {
		return s;
	}
	if (s[end] == '.') {
		// Every fractional digit was zero: keep exactly one of them.
		return s.substr(0, end + 2);
	}
	return s.substr(0, end + 1);
}

std::string LODTableUtils::NormalizeLOD(double lod) {
	std::ostringstream oss;
	oss << std::fixed << std::setprecision(12) << lod;
	return TrimTrailingZeros(oss.str());
}

std::string LODTableUtils::NormalizeLOD(const std::string &lod) {
	try {
		size_t idx = 0;
		double value = std::stod(lod, &idx);
		if (idx == lod.size()) {
			return NormalizeLOD(value);
		}
	} catch (...) {
		// Not a numeric LOD - return as-is
	}
	return lod;
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
	// Spec 02-object-table-schema.mdx, "Reserved columns": the head run, up to
	// but not including `bbox`.
	return {
	    Column("id", ColumnType::Varchar),          Column("feature_id", ColumnType::Varchar),
	    Column("object_type", ColumnType::Varchar), Column("parents", ColumnType::VarcharArray),
	    Column("children", ColumnType::VarcharArray), Column("children_roles", ColumnType::VarcharArray),
	    Column("address", ColumnType::AddressList),
	};
}

std::vector<Column> LODTableUtils::GetGeometryColumns(const std::string &lod) {
	// Spec § "Levels of detail": every geometry column is suffixed, and the suffix
	// always carries a minor -- LoD "3" reads back as "3.0" and yields
	// geometry_lod3_0. The single-LoD `lod=` mode therefore uses exactly the same
	// column grammar as the wide layout, just restricted to one LoD. That keeps the
	// level of detail recoverable from the column name alone, which is what lets
	// COPY TO cityjson re-emit it -- an un-suffixed layout had nowhere to put it.
	//
	// `bbox` leads the group: the spec places it immediately before the
	// geometry_lod* family, not after.
	std::string suffix = FormatLODAsColumnSuffix(lod);
	return {
	    Column("bbox", ColumnType::GeographicalExtent),
	    Column("geometry_" + suffix, ColumnType::GeometryWKB),
	    Column("geometry_properties_" + suffix, ColumnType::GeometryPropertiesStruct),
	    // Per-LoD appearance columns paired to the geometry by name (§11).
	    Column("material_" + suffix, ColumnType::AppearanceJson),
	    Column("texture_" + suffix, ColumnType::AppearanceJson),
	};
}

std::vector<Column> LODTableUtils::GetTrailingColumns() {
	// Spec 02-object-table-schema.mdx, "Reserved columns": everything after the
	// geometry group and before any attribute column, excluding the optional
	// `other_attributes` (this reader never synthesises one).
	return {
	    Column("template", ColumnType::TemplateStruct),
	    Column("other", ColumnType::Json),
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

		// Reserved columns first, in the spec's fixed order -- head, then bbox +
		// geometry (suffixed with this table's LoD), then the trailing run -- and
		// only then every attribute column.
		auto base_cols = GetBaseColumns();
		table.columns.insert(table.columns.end(), base_cols.begin(), base_cols.end());

		auto geom_cols = GetGeometryColumns(lod);
		table.columns.insert(table.columns.end(), geom_cols.begin(), geom_cols.end());

		auto trailing_cols = GetTrailingColumns();
		table.columns.insert(table.columns.end(), trailing_cols.begin(), trailing_cols.end());

		table.columns.insert(table.columns.end(), attribute_columns.begin(), attribute_columns.end());

		tables.push_back(std::move(table));
	}

	return tables;
}

} // namespace cityjson
} // namespace duckdb

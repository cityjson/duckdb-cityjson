#include "cityjson/column_types.hpp"
#include "cityjson/lod_table.hpp"
#include <algorithm>
#include <cctype>
#include <regex>

namespace duckdb {
namespace cityjson {

// ============================================================
// ColumnTypeUtils - Basic Conversions
// ============================================================

const char *ColumnTypeUtils::ToString(ColumnType type) {
	switch (type) {
	case ColumnType::Boolean:
		return "BOOLEAN";
	case ColumnType::BigInt:
		return "BIGINT";
	case ColumnType::Double:
		return "DOUBLE";
	case ColumnType::Varchar:
		return "VARCHAR";
	case ColumnType::Timestamp:
		return "TIMESTAMP";
	case ColumnType::Date:
		return "DATE";
	case ColumnType::Time:
		return "TIME";
	case ColumnType::Json:
		return "JSON";
	case ColumnType::VarcharArray:
		return "LIST(VARCHAR)";
	case ColumnType::Geometry:
		return "STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR, semantics VARCHAR, material VARCHAR, texture "
		       "VARCHAR)";
	case ColumnType::GeographicalExtent:
		return "STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE, max_x DOUBLE, max_y DOUBLE, max_z DOUBLE)";
	case ColumnType::GeometryWKB:
		return "BLOB";
	case ColumnType::GeometryPropertiesJson:
	case ColumnType::AppearanceJson:
		return "JSON";
	default:
		return "UNKNOWN";
	}
}

LogicalTypeId ColumnTypeUtils::ToLogicalTypeId(ColumnType type) {
	switch (type) {
	case ColumnType::Boolean:
		return LogicalTypeId::BOOLEAN;
	case ColumnType::BigInt:
		return LogicalTypeId::BIGINT;
	case ColumnType::Double:
		return LogicalTypeId::DOUBLE;
	case ColumnType::Varchar:
		return LogicalTypeId::VARCHAR;
	case ColumnType::Timestamp:
		return LogicalTypeId::TIMESTAMP;
	case ColumnType::Date:
		return LogicalTypeId::DATE;
	case ColumnType::Time:
		return LogicalTypeId::TIME;
	case ColumnType::Json:
		return LogicalTypeId::VARCHAR; // JSON stored as VARCHAR
	case ColumnType::VarcharArray:
		return LogicalTypeId::LIST;
	case ColumnType::Geometry:
		return LogicalTypeId::STRUCT;
	case ColumnType::GeographicalExtent:
		return LogicalTypeId::STRUCT;
	case ColumnType::GeometryWKB:
		return LogicalTypeId::BLOB;
	case ColumnType::GeometryPropertiesJson:
	case ColumnType::AppearanceJson:
		return LogicalTypeId::VARCHAR; // JSON stored as VARCHAR
	default:
		return LogicalTypeId::INVALID;
	}
}

LogicalType ColumnTypeUtils::ToDuckDBType(ColumnType type) {
	switch (type) {
	case ColumnType::Boolean:
		return LogicalType::BOOLEAN;
	case ColumnType::BigInt:
		return LogicalType::BIGINT;
	case ColumnType::Double:
		return LogicalType::DOUBLE;
	case ColumnType::Varchar:
		return LogicalType::VARCHAR;
	case ColumnType::Timestamp:
		return LogicalType::TIMESTAMP;
	case ColumnType::Date:
		return LogicalType::DATE;
	case ColumnType::Time:
		return LogicalType::TIME;
	case ColumnType::Json:
		return LogicalType::VARCHAR; // JSON stored as VARCHAR

	case ColumnType::VarcharArray: {
		// LIST(VARCHAR)
		return LogicalType::LIST(LogicalType::VARCHAR);
	}

	case ColumnType::Geometry: {
		// STRUCT(lod VARCHAR, type VARCHAR, boundaries VARCHAR,
		//        semantics VARCHAR, material VARCHAR, texture VARCHAR)
		child_list_t<LogicalType> children;
		children.push_back(std::make_pair("lod", LogicalType::VARCHAR));
		children.push_back(std::make_pair("type", LogicalType::VARCHAR));
		children.push_back(std::make_pair("boundaries", LogicalType::VARCHAR));
		children.push_back(std::make_pair("semantics", LogicalType::VARCHAR));
		children.push_back(std::make_pair("material", LogicalType::VARCHAR));
		children.push_back(std::make_pair("texture", LogicalType::VARCHAR));
		return LogicalType::STRUCT(children);
	}

	case ColumnType::GeographicalExtent: {
		// STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE,
		//        max_x DOUBLE, max_y DOUBLE, max_z DOUBLE)
		child_list_t<LogicalType> children;
		children.push_back(std::make_pair("min_x", LogicalType::DOUBLE));
		children.push_back(std::make_pair("min_y", LogicalType::DOUBLE));
		children.push_back(std::make_pair("min_z", LogicalType::DOUBLE));
		children.push_back(std::make_pair("max_x", LogicalType::DOUBLE));
		children.push_back(std::make_pair("max_y", LogicalType::DOUBLE));
		children.push_back(std::make_pair("max_z", LogicalType::DOUBLE));
		return LogicalType::STRUCT(children);
	}

	case ColumnType::GeometryWKB:
		return LogicalType::BLOB;

	case ColumnType::GeometryPropertiesJson:
	case ColumnType::AppearanceJson:
		return LogicalType::VARCHAR; // JSON stored as VARCHAR

	default:
		return LogicalType::INVALID;
	}
}

ColumnType ColumnTypeUtils::Parse(const std::string &name) {
	// Convert to lowercase for case-insensitive comparison
	std::string lower_name = name;
	std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(),
	               [](unsigned char c) { return std::tolower(c); });

	// Boolean types
	if (lower_name == "boolean" || lower_name == "bool") {
		return ColumnType::Boolean;
	}

	// Integer types
	if (lower_name == "bigint" || lower_name == "int" || lower_name == "integer" || lower_name == "int64") {
		return ColumnType::BigInt;
	}

	// Floating point types
	if (lower_name == "double" || lower_name == "float" || lower_name == "float64" || lower_name == "real") {
		return ColumnType::Double;
	}

	// String types
	if (lower_name == "varchar" || lower_name == "text" || lower_name == "string" || lower_name == "str") {
		return ColumnType::Varchar;
	}

	// Temporal types
	if (lower_name == "timestamp" || lower_name == "datetime") {
		return ColumnType::Timestamp;
	}
	if (lower_name == "date") {
		return ColumnType::Date;
	}
	if (lower_name == "time") {
		return ColumnType::Time;
	}

	// Complex types
	if (lower_name == "json") {
		return ColumnType::Json;
	}
	if (lower_name == "list(varchar)" || lower_name == "varchar[]" || lower_name == "string[]" ||
	    lower_name == "varchararray") {
		return ColumnType::VarcharArray;
	}
	if (lower_name == "geometry") {
		return ColumnType::Geometry;
	}
	if (lower_name == "geographicalextent" || lower_name == "extent") {
		return ColumnType::GeographicalExtent;
	}
	if (lower_name == "geometrywkb" || lower_name == "wkb" || lower_name == "blob") {
		return ColumnType::GeometryWKB;
	}
	if (lower_name == "geometrypropertiesjson" || lower_name == "geometry_properties") {
		return ColumnType::GeometryPropertiesJson;
	}

	// Not found - throw error
	throw CityJSONError::Conversion("Unknown column type: " + name);
}

// ============================================================
// ColumnTypeUtils - Type Inference
// ============================================================

std::optional<ColumnType> ColumnTypeUtils::InferTemporalType(const std::string &str) {
	// ISO 8601 timestamp pattern: YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD HH:MM:SS
	// With optional fractional seconds and timezone
	std::regex timestamp_pattern(R"(\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2})");
	if (std::regex_search(str, timestamp_pattern)) {
		return ColumnType::Timestamp;
	}

	// Date pattern: YYYY-MM-DD
	std::regex date_pattern(R"(^\d{4}-\d{2}-\d{2}$)");
	if (std::regex_match(str, date_pattern)) {
		return ColumnType::Date;
	}

	// Time pattern: HH:MM:SS
	std::regex time_pattern(R"(^\d{2}:\d{2}:\d{2})");
	if (std::regex_match(str, time_pattern)) {
		return ColumnType::Time;
	}

	return std::nullopt;
}

ColumnType ColumnTypeUtils::InferFromJson(const json &value) {
	// Handle null - use Varchar as fallback
	if (value.is_null()) {
		return ColumnType::Varchar;
	}

	// Boolean
	if (value.is_boolean()) {
		return ColumnType::Boolean;
	}

	// Number (integer)
	if (value.is_number_integer()) {
		return ColumnType::BigInt;
	}

	// Number (floating point)
	if (value.is_number_float()) {
		return ColumnType::Double;
	}

	// String - check for temporal patterns
	if (value.is_string()) {
		std::string str = value.get<std::string>();
		auto temporal_type = InferTemporalType(str);
		if (temporal_type.has_value()) {
			return temporal_type.value();
		}
		return ColumnType::Varchar;
	}

	// Array - check if all elements are strings
	if (value.is_array()) {
		if (value.empty()) {
			return ColumnType::Json; // Empty array - use JSON
		}

		bool all_strings = true;
		for (const auto &elem : value) {
			if (!elem.is_string()) {
				all_strings = false;
				break;
			}
		}

		if (all_strings) {
			return ColumnType::VarcharArray;
		} else {
			return ColumnType::Json;
		}
	}

	// Object - use JSON
	if (value.is_object()) {
		return ColumnType::Json;
	}

	// Fallback
	return ColumnType::Varchar;
}

ColumnType ColumnTypeUtils::ResolveFromSamples(const std::vector<ColumnType> &types) {
	if (types.empty()) {
		return ColumnType::Varchar; // Default fallback
	}

	// Check if all types are the same
	bool all_same = true;
	ColumnType first = types[0];
	for (const auto &type : types) {
		if (type != first) {
			all_same = false;
			break;
		}
	}

	if (all_same) {
		return first;
	}

	// Check for numeric promotion (BigInt + Double -> Double)
	bool has_bigint = false;
	bool has_double = false;
	bool has_other = false;

	for (const auto &type : types) {
		if (type == ColumnType::BigInt) {
			has_bigint = true;
		} else if (type == ColumnType::Double) {
			has_double = true;
		} else {
			has_other = true;
		}
	}

	// If only BigInt and Double, promote to Double
	if ((has_bigint || has_double) && !has_other) {
		return ColumnType::Double;
	}

	// Otherwise, fall back to Varchar for inconsistency
	return ColumnType::Varchar;
}

// ============================================================
// ColumnTypeUtils - Helper Methods
// ============================================================

bool ColumnTypeUtils::IsNumeric(ColumnType type) {
	return type == ColumnType::BigInt || type == ColumnType::Double;
}

bool ColumnTypeUtils::IsTemporal(ColumnType type) {
	return type == ColumnType::Timestamp || type == ColumnType::Date || type == ColumnType::Time;
}

bool ColumnTypeUtils::IsComplex(ColumnType type) {
	return type == ColumnType::Json || type == ColumnType::VarcharArray || type == ColumnType::Geometry ||
	       type == ColumnType::GeographicalExtent || type == ColumnType::GeometryWKB ||
	       type == ColumnType::GeometryPropertiesJson || type == ColumnType::AppearanceJson;
}

// ============================================================
// Predefined Columns
// ============================================================

std::vector<Column> GetDefinedColumns() {
	return {
	    Column("id", ColumnType::Varchar),
	    Column("feature_id", ColumnType::Varchar),
	    Column("object_type", ColumnType::Varchar),
	    Column("children", ColumnType::VarcharArray),
	    Column("children_roles", ColumnType::VarcharArray),
	    Column("parents", ColumnType::VarcharArray),
	    Column("other", ColumnType::Json),
	};
}

bool IsPredefinedColumn(const std::string &name) {
	static const std::vector<std::string> predefined = {
	    "id", "feature_id", "object_type", "children", "children_roles", "parents", "other"};

	return std::find(predefined.begin(), predefined.end(), name) != predefined.end();
}

static std::string ToLowerAscii(const std::string &name) {
	std::string lowered = name;
	std::transform(lowered.begin(), lowered.end(), lowered.begin(),
	               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
	return lowered;
}

bool IsReservedColumnName(const std::string &name) {
	static const std::vector<std::string> reserved = {"id",       "feature_id", "object_type", "children",
	                                                  "children_roles", "parents", "other",       "bbox",
	                                                  "geometry"};
	const std::string lowered = ToLowerAscii(name);
	if (std::find(reserved.begin(), reserved.end(), lowered) != reserved.end()) {
		return true;
	}
	// Wide-layout per-LOD structural columns: geometry_lod*, geometry_properties*,
	// and the paired appearance columns material_lod* / texture_lod* (§11).
	return lowered.rfind("geometry_lod", 0) == 0 || lowered.rfind("geometry_properties", 0) == 0 ||
	       lowered.rfind("material_lod", 0) == 0 || lowered.rfind("texture_lod", 0) == 0 || lowered == "material" ||
	       lowered == "texture";
}

bool IsGeometryColumn(const std::string &name) {
	// Pattern: geom_lod{X} or geom_lod{X}_{Y}
	std::regex geom_pattern(R"(geom_lod\d+(_\d+)?)");
	return std::regex_match(name, geom_pattern);
}

std::string ParseLODFromColumnName(const std::string &column_name) {
	// Parse "geom_lod2_1" -> "2.1"
	static const std::regex lod_pattern(R"(geom_lod(\d+)_?(\d*))");
	std::smatch match;

	if (std::regex_match(column_name, match, lod_pattern)) {
		std::string lod = match[1].str();
		if (match[2].length() > 0) {
			lod += "." + match[2].str();
		}
		return LODTableUtils::NormalizeLOD(lod);
	}

	throw CityJSONError::InvalidSchema("Invalid geometry column name: " + column_name);
}

std::string ParseLODFromGeometryColumn(const std::string &column_name) {
	// Matches the LOD component of "geometry_lod2_2" / "geometry_properties_lod2_2"
	// and single-component "geometry_lod0".
	static const std::regex lod_pattern(R"(lod(\d+)_?(\d*))");
	std::smatch match;

	if (std::regex_search(column_name, match, lod_pattern)) {
		std::string lod = match[1].str();
		if (match[2].length() > 0) {
			lod += "." + match[2].str();
		}
		return LODTableUtils::NormalizeLOD(lod);
	}

	throw CityJSONError::InvalidSchema("Invalid geometry column name: " + column_name);
}

} // namespace cityjson
} // namespace duckdb

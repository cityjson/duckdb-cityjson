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
		// A CityJSON date-time is an ISO 8601 instant, so this column MUST be
		// UTC-adjusted (spec 02-object-table-schema.mdx "Temporal columns");
		// DuckDB's timezone-naive TIMESTAMP would denote a wall-clock reading,
		// a different quantity.
		return "TIMESTAMP WITH TIME ZONE";
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
		return "STRUCT(xmin DOUBLE, ymin DOUBLE, zmin DOUBLE, xmax DOUBLE, ymax DOUBLE, zmax DOUBLE)";
	case ColumnType::GeometryWKB:
		return "BLOB";
	case ColumnType::GeometryPropertiesStruct:
		// `surfaces` holds JSON text; see ToDuckDBType for why it is typed VARCHAR.
		return "STRUCT(\"type\" VARCHAR, surfaces VARCHAR, face_semantics INTEGER[], shells INTEGER[][])";
	case ColumnType::AppearanceJson:
		return "JSON";
	case ColumnType::GeometryArrowNative:
		return "INTEGER[][][][][]";
	case ColumnType::GeometryVerticesArrowNative:
		return "STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)[]";
	case ColumnType::AddressList:
		return "STRUCT(street VARCHAR, house_number VARCHAR, po_box VARCHAR, zip_code VARCHAR, city VARCHAR, "
		       "state VARCHAR, country VARCHAR, free_text VARCHAR, location BLOB)[]";
	case ColumnType::TemplateStruct:
		return "STRUCT(id BIGINT, point BLOB, transformationMatrix DOUBLE[])";
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
		return LogicalTypeId::TIMESTAMP_TZ;
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
	case ColumnType::GeometryPropertiesStruct:
		return LogicalTypeId::STRUCT;
	case ColumnType::AppearanceJson:
		return LogicalTypeId::VARCHAR; // JSON stored as VARCHAR
	case ColumnType::GeometryArrowNative:
		return LogicalTypeId::LIST;
	case ColumnType::GeometryVerticesArrowNative:
		return LogicalTypeId::LIST;
	case ColumnType::AddressList:
		return LogicalTypeId::LIST;
	case ColumnType::TemplateStruct:
		return LogicalTypeId::STRUCT;
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
		return LogicalType::TIMESTAMP_TZ;
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
		// STRUCT(xmin DOUBLE, ymin DOUBLE, zmin DOUBLE,
		//        xmax DOUBLE, ymax DOUBLE, zmax DOUBLE)
		// Field names follow GeoParquet's bbox convention (spec
		// 02-object-table-schema.mdx): this is the row-level `bbox` column's type,
		// not the metadata table's `geographical_extent`, which keeps its own
		// separate min_x/max_x naming in metadata_table.cpp.
		child_list_t<LogicalType> children;
		children.push_back(std::make_pair("xmin", LogicalType::DOUBLE));
		children.push_back(std::make_pair("ymin", LogicalType::DOUBLE));
		children.push_back(std::make_pair("zmin", LogicalType::DOUBLE));
		children.push_back(std::make_pair("xmax", LogicalType::DOUBLE));
		children.push_back(std::make_pair("ymax", LogicalType::DOUBLE));
		children.push_back(std::make_pair("zmax", LogicalType::DOUBLE));
		return LogicalType::STRUCT(children);
	}

	case ColumnType::GeometryWKB:
		return LogicalType::BLOB;

	case ColumnType::GeometryPropertiesStruct: {
		// Spec § "Geometry properties and semantics": the CityGML CM information WKB
		// has no slot for. `surfaces` stays JSON because semantic surfaces have no
		// fixed shape (each may carry different attributes); the rest are typed so a
		// query engine reads them without parsing. There is deliberately no `lod`
		// field -- the column name carries the level of detail.
		// `surfaces` is VARCHAR rather than LogicalType::JSON(). The spec's "JSON"
		// names the logical content, not a physical annotation, and DuckDB's JSON
		// type buys nothing here while costing real usability: it is a VARCHAR alias
		// whose operators (`->`, `.field`) bind to json_extract from the `json`
		// extension, which this extension does not depend on and which is not loaded
		// by default -- so even `surfaces LIKE '%RoofSurface%'` fails to bind. It
		// would not buy interoperability either: cityparquet-rs marks the same field
		// with the Arrow extension name `arrow.json`, not the Parquet JSON logical
		// type DuckDB emits, so the two never agreed via this mechanism anyway.
		child_list_t<LogicalType> children;
		children.push_back(std::make_pair("type", LogicalType::VARCHAR));
		children.push_back(std::make_pair("surfaces", LogicalType::VARCHAR));
		children.push_back(std::make_pair("face_semantics", LogicalType::LIST(LogicalType::INTEGER)));
		children.push_back(std::make_pair("shells", LogicalType::LIST(LogicalType::LIST(LogicalType::INTEGER))));
		return LogicalType::STRUCT(children);
	}

	case ColumnType::AppearanceJson:
		return LogicalType::VARCHAR; // JSON stored as VARCHAR

	case ColumnType::GeometryArrowNative: {
		// solid -> shell -> face -> ring -> vertex-pool index: five LIST levels,
		// matching cityparquet-rs's arrow_native_geometry_data_type().
		//
		// The outer two levels are PHYSICAL ONLY for the surface families: a
		// MultiSurface/CompositeSurface pads them both to length 1, and they carry no
		// solid/shell/cavity meaning there. They are real structure for the solid
		// families. Nothing may infer the CityJSON type from this nesting -- that is
		// what geometry_properties_lod*.type is for (design doc, "Critical invariant").
		auto ring = LogicalType::LIST(LogicalType::INTEGER);
		auto face = LogicalType::LIST(ring);
		auto shell = LogicalType::LIST(face);
		auto solid = LogicalType::LIST(shell);
		return LogicalType::LIST(solid);
	}

	case ColumnType::GeometryVerticesArrowNative: {
		// This row's vertex pool, built by compacting the DISTINCT SOURCE INDICES the
		// object's geometry references into a dense local range -- never by comparing
		// coordinate values. CityJSON permits two distinct indices to carry identical
		// coordinates, and those stay two entries (design doc, round-2 review item 1).
		//
		// STRUCT(x, y, z) rather than a fixed-size list of 3: Parquet shreds struct
		// fields into independent leaf columns, giving per-axis statistics and letting
		// homogeneous per-axis doubles compress on their own.
		child_list_t<LogicalType> children;
		children.push_back(std::make_pair("x", LogicalType::DOUBLE));
		children.push_back(std::make_pair("y", LogicalType::DOUBLE));
		children.push_back(std::make_pair("z", LogicalType::DOUBLE));
		return LogicalType::LIST(LogicalType::STRUCT(children));
	}

	case ColumnType::AddressList: {
		// Spec "Addresses": a lean subset of 3DCityDB v5's ADDRESS table. `location`
		// is a WKB MultiPointZ in the file CRS -- geometry, not a vertex-pool
		// reference, so it is a BLOB like any other geometry column.
		child_list_t<LogicalType> fields;
		fields.push_back(std::make_pair("street", LogicalType::VARCHAR));
		fields.push_back(std::make_pair("house_number", LogicalType::VARCHAR));
		fields.push_back(std::make_pair("po_box", LogicalType::VARCHAR));
		fields.push_back(std::make_pair("zip_code", LogicalType::VARCHAR));
		fields.push_back(std::make_pair("city", LogicalType::VARCHAR));
		fields.push_back(std::make_pair("state", LogicalType::VARCHAR));
		fields.push_back(std::make_pair("country", LogicalType::VARCHAR));
		fields.push_back(std::make_pair("free_text", LogicalType::VARCHAR));
		fields.push_back(std::make_pair("location", LogicalType::BLOB));
		return LogicalType::LIST(LogicalType::STRUCT(fields));
	}

	case ColumnType::TemplateStruct: {
		// Spec: geometry-template instance data. The matrix is a flat 16-element
		// row-major 4x4, not a nested type -- there is no per-writer choice to
		// preserve, and DOUBLE[] is what a consumer actually wants to index into.
		child_list_t<LogicalType> children;
		children.push_back(std::make_pair("id", LogicalType::BIGINT));
		children.push_back(std::make_pair("point", LogicalType::BLOB));
		children.push_back(std::make_pair("transformationMatrix", LogicalType::LIST(LogicalType::DOUBLE)));
		return LogicalType::STRUCT(children);
	}

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
		return ColumnType::GeometryPropertiesStruct;
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
	       type == ColumnType::GeometryPropertiesStruct || type == ColumnType::AppearanceJson ||
	       type == ColumnType::AddressList || type == ColumnType::TemplateStruct;
}

// ============================================================
// Predefined Columns
// ============================================================

std::vector<Column> GetDefinedColumns() {
	// Spec 02-object-table-schema.mdx, "Reserved columns": this is the leading
	// (head) run of the reserved order, up to but not including `bbox` -- callers
	// splice geometry columns after this and LODTableUtils::GetTrailingColumns()
	// (`template`, `other`) after that, before any attribute column.
	return {
	    Column("id", ColumnType::Varchar),
	    Column("feature_id", ColumnType::Varchar),
	    Column("object_type", ColumnType::Varchar),
	    Column("parents", ColumnType::VarcharArray),
	    Column("children", ColumnType::VarcharArray),
	    Column("children_roles", ColumnType::VarcharArray),
	    Column("address", ColumnType::AddressList),
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
	static const std::vector<std::string> reserved = {
	    "id",    "feature_id", "object_type", "children", "children_roles", "parents",
	    "other", "bbox",       "geometry",    "address",  "template",       "other_attributes"};
	const std::string lowered = ToLowerAscii(name);
	if (std::find(reserved.begin(), reserved.end(), lowered) != reserved.end()) {
		return true;
	}
	// Wide-layout per-LOD structural columns: geometry_lod*, geometry_properties*,
	// and the paired appearance columns material_lod* / texture_lod* (§11).
	// geometry_vertices* is reserved too: the arrow-native encoding generates one
	// per geometry column, and an attribute of that name would collide with it.
	// Reserved under either encoding, so a file does not change which of its
	// attributes get columns depending on how it is read.
	return lowered.rfind("geometry_lod", 0) == 0 || lowered.rfind("geometry_properties", 0) == 0 ||
	       lowered.rfind("geometry_vertices", 0) == 0 || IsAppearanceColumnName(lowered);
}

bool IsAppearanceColumnName(const std::string &name) {
	// Exactly `material`/`texture`, or those with the reserved LoD suffix grammar
	// `_lod{X}` / `_lod{X}_{Y}`. Anchored so `material_lodging` does NOT match.
	static const std::regex appearance_pattern(R"((material|texture)(_lod\d+(_\d+)?)?)");
	return std::regex_match(ToLowerAscii(name), appearance_pattern);
}

bool IsGeometryColumn(const std::string &name) {
	// Pattern: geom_lod{X} or geom_lod{X}_{Y}. static: regex construction compiles
	// the pattern, and this runs per column in hot paths.
	static const std::regex geom_pattern(R"(geom_lod\d+(_\d+)?)");
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

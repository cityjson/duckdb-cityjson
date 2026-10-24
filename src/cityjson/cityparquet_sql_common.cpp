#include "cityjson/cityparquet_sql_common.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/keyword_helper.hpp"

namespace duckdb {
namespace cityjson {

std::string Join(const std::vector<std::string> &parts, const std::string &separator) {
	std::string out;
	for (idx_t i = 0; i < parts.size(); i++) {
		if (i > 0) {
			out += separator;
		}
		out += parts[i];
	}
	return out;
}

std::string Quoted(const std::string &name) {
	return KeywordHelper::WriteOptionallyQuoted(name);
}

bool HasSurfacesField(const LogicalType &type) {
	if (type.id() != LogicalTypeId::STRUCT) {
		return false;
	}
	for (const auto &child : StructType::GetChildTypes(type)) {
		if (child.first == "surfaces" && child.second.id() == LogicalTypeId::VARCHAR) {
			return true;
		}
	}
	return false;
}

std::vector<ColumnInfo> TableColumns(ClientContext &context, const std::string &schema, const std::string &table) {
	std::vector<ColumnInfo> columns;
	// The non-templated GetEntry: Catalog::GetEntry<TableCatalogEntry> ODR-uses
	// TableCatalogEntry::Name and collides with DuckDB's own definition at link time.
	auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, INVALID_CATALOG, schema, table);
	for (auto &column : entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		columns.push_back({column.Name(), column.Type()});
	}
	return columns;
}

const ColumnInfo *FindColumn(const std::vector<ColumnInfo> &columns, const std::string &name) {
	for (const auto &column : columns) {
		if (StringUtil::Lower(column.name) == StringUtil::Lower(name)) {
			return &column;
		}
	}
	return nullptr;
}

std::string ShiftedImplicitGeometry(const std::string &offset_expr) {
	return "CASE WHEN implicit_geometry IS NULL THEN NULL ELSE struct_pack(id := implicit_geometry.id + " +
	       offset_expr +
	       ", point := implicit_geometry.point, \"transformationMatrix\" := "
	       "implicit_geometry.\"transformationMatrix\") "
	       "END";
}

LogicalType WithoutJsonAlias(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::VARCHAR:
		return LogicalType(LogicalTypeId::VARCHAR);
	case LogicalTypeId::STRUCT: {
		child_list_t<LogicalType> children;
		for (const auto &child : StructType::GetChildTypes(type)) {
			children.emplace_back(child.first, WithoutJsonAlias(child.second));
		}
		return LogicalType::STRUCT(std::move(children));
	}
	case LogicalTypeId::LIST:
		return LogicalType::LIST(WithoutJsonAlias(ListType::GetChildType(type)));
	case LogicalTypeId::MAP:
		return LogicalType::MAP(WithoutJsonAlias(MapType::KeyType(type)), WithoutJsonAlias(MapType::ValueType(type)));
	default:
		return type;
	}
}

std::string SqlTypeName(const LogicalType &type) {
	if (type.IsJSONType()) {
		return "CITYPARQUET_JSON";
	}
	switch (type.id()) {
	case LogicalTypeId::STRUCT: {
		vector<string> fields;
		for (const auto &child : StructType::GetChildTypes(type)) {
			fields.push_back(Quoted(child.first) + " " + SqlTypeName(child.second));
		}
		return "STRUCT(" + StringUtil::Join(fields, ", ") + ")";
	}
	case LogicalTypeId::LIST:
		return SqlTypeName(ListType::GetChildType(type)) + "[]";
	case LogicalTypeId::MAP:
		return "MAP(" + SqlTypeName(MapType::KeyType(type)) + ", " + SqlTypeName(MapType::ValueType(type)) + ")";
	default:
		return type.ToString();
	}
}

std::string WidenedUsing(const LogicalType &from, const LogicalType &to, const std::string &column_name) {
	if (!NeedsJsonEncoding(to, from, column_name)) {
		return "";
	}
	return " USING cityparquet_to_json(" + Quoted(column_name) + ")";
}

bool NeedsJsonEncoding(const LogicalType &destination, const LogicalType &source, const std::string &column_name) {
	return destination.IsJSONType() && !source.IsJSONType() && !StringUtil::CIEquals(column_name, "other");
}

LogicalType WidenedType(const LogicalType &destination, const LogicalType &source, const std::string &function,
                        const std::string &column_name) {
	// An attribute column typed JSON on one side only: one side holds structured
	// values, so the column is JSON (spec 02-object-table-schema.mdx, "Attribute types
	// and promotion": "JSON for structured values"), and the other side's values are
	// encoded as the JSON they stand for (cityparquet_to_json). `other` is JSON on both
	// sides whatever its type says: the readers hand it over as VARCHAR.
	const bool json_side = destination.IsJSONType() != source.IsJSONType();
	const auto &plain = destination.IsJSONType() ? source : destination;
	const bool encodable = plain.id() != LogicalTypeId::GEOMETRY && plain.id() != LogicalTypeId::BLOB &&
	                       plain.id() != LogicalTypeId::STRUCT && plain.id() != LogicalTypeId::MAP;
	if (json_side && encodable && !StringUtil::CIEquals(column_name, "other")) {
		return destination.IsJSONType() ? LogicalType(LogicalTypeId::INVALID) : LogicalType::JSON();
	}
	// JSON is text with a name: a package loaded from a file that declares the JSON
	// logical type holds it as JSON, a CityJSON reader as VARCHAR, and the two are the
	// same column.
	if (WithoutJsonAlias(destination) == WithoutJsonAlias(source)) {
		return LogicalType(LogicalTypeId::INVALID);
	}
	const auto d = destination.id();
	const auto s = source.id();
	const auto is_nested = [](LogicalTypeId id) {
		return id == LogicalTypeId::STRUCT || id == LogicalTypeId::LIST || id == LogicalTypeId::MAP;
	};
	const bool d_int = d == LogicalTypeId::BIGINT || d == LogicalTypeId::INTEGER;
	const bool s_double = s == LogicalTypeId::DOUBLE || s == LogicalTypeId::FLOAT;
	if (d_int && s_double) {
		return LogicalType(LogicalTypeId::DOUBLE);
	}
	if (d == LogicalTypeId::DOUBLE && (s == LogicalTypeId::BIGINT || s == LogicalTypeId::INTEGER)) {
		return LogicalType(LogicalTypeId::INVALID); // destination already wider
	}
	if (d == LogicalTypeId::VARCHAR && !is_nested(s) && s != LogicalTypeId::GEOMETRY && s != LogicalTypeId::BLOB) {
		return LogicalType(LogicalTypeId::INVALID); // text already holds any scalar
	}
	if (is_nested(d) || is_nested(s)) {
		throw BinderException("%s: column '%s' cannot be widened -- the destination is %s and the incoming type "
		                      "is %s. A nested type disagreeing in shape means the two sides disagree about the "
		                      "package's own schema; that is not something to paper over by stringifying a "
		                      "reserved structural column",
		                      function, column_name, destination.ToString(), source.ToString());
	}
	if (d == LogicalTypeId::GEOMETRY || s == LogicalTypeId::GEOMETRY) {
		throw BinderException("%s: column '%s' cannot be widened -- the destination is %s and the incoming type "
		                      "is %s. A geometry column holds WKB, never text; stringifying it would leave "
		                      "nothing a geometry function or cityparquet_write could read",
		                      function, column_name, destination.ToString(), source.ToString());
	}
	return LogicalType(LogicalTypeId::VARCHAR);
}

void LogWidening(ClientContext &context, const std::string &function, const std::string &table,
                 const std::string &column_name, const LogicalType &from, const LogicalType &incoming,
                 const LogicalType &to) {
	DUCKDB_LOG_WARNING(context,
	                   "%s: column '%s' of %s holds %s and the incoming rows %s, so the column is widened to %s for "
	                   "every row, the promotion the specification gives mixed attribute types",
	                   function, column_name, table, from.ToString(), incoming.ToString(), to.ToString());
}

std::string GeometryColumnRef(const std::string &quoted_name, const LogicalType &type) {
	if (type.id() == LogicalTypeId::GEOMETRY) {
		return "ST_AsWKB(" + quoted_name + ")";
	}
	return quoted_name;
}

namespace {

//! The object-table footers that actually declare a CRS -- the only rows either helper
//! below may read. See the header for why the sidecars and the NULLs have to go.
std::string DeclaringObjectFooters(const std::string &schema) {
	return " FROM " + QualifiedName(schema, "__cityparquet") +
	       " WHERE role = 'object' AND city IS NOT NULL AND cityparquet_city_field(city, 'crs') IS NOT NULL";
}

} // namespace

std::string DeclaredCrsExpr(const std::string &schema) {
	// max(), not DISTINCT: one row whatever the data says. With OneCrsPerPackageSQL run
	// first there is at most one distinct value for it to pick, so which one is moot.
	return "(SELECT max(cityparquet_city_field(city, 'crs'))" + DeclaringObjectFooters(schema) + ")";
}

std::string CrsStatedExpr(const std::string &schema) {
	return "(SELECT COUNT(*) > 0 FROM " + QualifiedName(schema, "__cityparquet") +
	       " WHERE role = 'object' AND city IS NOT NULL AND (cityparquet_city_field(city, 'version') IS NOT NULL OR "
	       "cityparquet_city_field(city, 'crs') IS NOT NULL))";
}

std::string OneCrsPerPackageSQL(const std::string &function, const std::string &schema, const std::string &label) {
	return "SELECT error('" + function + ": the " + label +
	       " package declares more than one CRS -- its object-table footers disagree, and a package '\n"
	       "  'states one CRS for every row it holds; rewrite it with cityparquet_write(..., crs => ...)') FROM (\n"
	       "  SELECT COUNT(DISTINCT cityparquet_city_field(city, 'crs')) AS n" +
	       DeclaringObjectFooters(schema) + "\n) WHERE n > 1;\n";
}

std::string CrsPreconditionSQL(const CrsCheckWording &wording, const std::string &destination_crs_expr,
                               const std::string &destination_stated_expr, const std::string &source_crs_expr,
                               const std::string &source_stated_expr) {
	const auto &fn = wording.function;
	const auto &noun = wording.source_noun;
	return "SELECT error(CASE\n"
	       "  WHEN d IS NOT NULL AND s IS NOT NULL THEN\n"
	       "    '" +
	       fn + ": CRS mismatch -- the destination is ' || d || ' and " + noun +
	       " is ' || s || '; reprojection is not performed'\n"
	       "  WHEN d IS NOT NULL THEN\n"
	       "    '" +
	       fn + ": the destination is ' || d || ' but " + noun +
	       " declares no CRS this writer can resolve, so its CRS is unknown -- a package '\n"
	       "    'states one CRS for every row it holds, and an unknown cannot be shown to be that one; " +
	       wording.source_unknown_hint +
	       "'\n"
	       "  ELSE\n"
	       "    '" +
	       fn +
	       ": the destination package CRS is unknown (its footer declares crs: null, or carries no crs at '\n"
	       "    'all) and " +
	       noun + " is ' || s || ' -- a package states one CRS for every row it holds; " +
	       wording.destination_unknown_hint +
	       "'\n"
	       "END) FROM (SELECT\n  " +
	       destination_crs_expr + " AS d,\n  " + source_crs_expr + " AS s\n) WHERE " + destination_stated_expr +
	       " AND " + source_stated_expr + " AND d IS DISTINCT FROM s;\n";
}

} // namespace cityjson
} // namespace duckdb

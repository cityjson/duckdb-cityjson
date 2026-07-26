#include "cityjson/cityparquet_validate.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <vector>

namespace duckdb {
namespace cityjson {

const char *const VALIDATION_TABLE = "cityparquet_validation";
const char *const ORPHAN_TABLE = "cityparquet_orphan_rows";

namespace {

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

bool HasColumn(ClientContext &context, const std::string &schema, const std::string &table,
               const std::string &column) {
	auto &catalog_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, INVALID_CATALOG, schema, table);
	for (auto &existing : catalog_entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		if (StringUtil::Lower(existing.Name()) == column) {
			return true;
		}
	}
	return false;
}

} // namespace

std::string ReferencedIds(ClientContext &context, const std::string &schema, const std::string &sidecar) {
	auto object_tables = ObjectTablesInSchema(context, schema);
	std::vector<std::string> terms;

	if (sidecar == "geometry_templates") {
		// A template reference is a plain struct field, no JSON involved. A table
		// predating the template column simply contributes no references.
		for (const auto &table : object_tables) {
			if (!HasColumn(context, schema, table, "template")) {
				continue;
			}
			terms.push_back("SELECT template.id AS ref FROM " + QualifiedName(schema, table) +
			                " WHERE template IS NOT NULL");
		}
	} else {
		const std::string prefix = (sidecar == "materials") ? "material_lod" : "texture_lod";
		const std::string kind = (sidecar == "materials") ? "material" : "texture";
		for (const auto &table : object_tables) {
			// Which LoDs a table carries is a property of the dataset, so the appearance
			// columns are discovered from the catalog rather than assumed.
			for (const auto &column : AppearanceLodColumns(context, schema, table, prefix)) {
				terms.push_back("SELECT UNNEST(cityjson_appearance_ids(" +
				                KeywordHelper::WriteOptionallyQuoted(column) + ", '" + kind + "')) AS ref FROM " +
				                QualifiedName(schema, table) + " WHERE " +
				                KeywordHelper::WriteOptionallyQuoted(column) + " IS NOT NULL");
			}
		}
	}

	if (terms.empty()) {
		// Nothing in the package can reference this sidecar, so every row in it is
		// unreferenced. Returning a typed empty set says exactly that.
		return "SELECT NULL WHERE false";
	}
	return Join(terms, "\nUNION\n");
}

std::string BuildOrphansSQL(ClientContext &context, const std::string &schema) {
	auto sidecars = SidecarTablesInSchema(context, schema);

	std::string body;
	if (sidecars.empty()) {
		body = "SELECT NULL::VARCHAR AS table_name, NULL::VARCHAR AS id, NULL::VARCHAR AS reason WHERE false";
	} else {
		std::vector<std::string> parts;
		for (const auto &sidecar : sidecars) {
			parts.push_back("SELECT " + KeywordHelper::WriteQuoted(sidecar, '\'') + " AS table_name, "
			                "CAST(s.id AS VARCHAR) AS id, 'unreferenced' AS reason FROM " +
			                QualifiedName(schema, sidecar) + " s WHERE s.id NOT IN (" +
			                ReferencedIds(context, schema, sidecar) + ")");
		}
		body = Join(parts, "\nUNION ALL\n");
	}

	std::string sql = "CREATE OR REPLACE TEMP TABLE " + std::string(ORPHAN_TABLE) + " AS\n" + body + ";\n";
	sql += "SELECT * FROM " + std::string(ORPHAN_TABLE) + " ORDER BY table_name, id;";
	return sql;
}

std::string BuildVacuumSQL(ClientContext &context, const std::string &schema) {
	auto sidecars = SidecarTablesInSchema(context, schema);
	if (sidecars.empty()) {
		return "SELECT 1 WHERE false;";
	}
	std::string sql;
	for (const auto &sidecar : sidecars) {
		// NOT IN: vacuum removes what nothing references. Inverting this would delete
		// precisely the rows still in use.
		sql += "DELETE FROM " + QualifiedName(schema, sidecar) + " WHERE id NOT IN (" +
		       ReferencedIds(context, schema, sidecar) + ");\n";
	}
	return sql;
}

namespace {

// Every check is written once against the `all_objects` union rather than once per
// module table. The columns these checks touch -- id, feature_id, parents, children,
// children_roles -- are common to every module; only attribute columns differ.
std::vector<std::string> Checks() {
	return {
	    "SELECT 'feature_id_null' AS check_name, 'error' AS severity, __tbl AS table_name, "
	    "id AS object_id, 'feature_id is NULL' AS message "
	    "FROM all_objects WHERE feature_id IS NULL",

	    "SELECT 'feature_id_dangling', 'error', __tbl, id, "
	    "'feature_id ' || feature_id || ' matches no object id in the package' "
	    "FROM all_objects WHERE feature_id IS NOT NULL "
	    "AND feature_id NOT IN (SELECT id FROM all_objects)",

	    // A parent or child reference resolves by bare id across every module file, so
	    // these anti-joins must span the whole package, not one table.
	    "SELECT 'parent_dangling', 'error', __tbl, id, "
	    "'parent ' || p || ' matches no object id in the package' "
	    "FROM all_objects, UNNEST(parents) AS t(p) "
	    "WHERE p IS NOT NULL AND p NOT IN (SELECT id FROM all_objects)",

	    "SELECT 'child_dangling', 'error', __tbl, id, "
	    "'child ' || c || ' matches no object id in the package' "
	    "FROM all_objects, UNNEST(children) AS t(c) "
	    "WHERE c IS NOT NULL AND c NOT IN (SELECT id FROM all_objects)",

	    "SELECT 'children_roles_misaligned', 'error', __tbl, id, "
	    "'children_roles has ' || len(children_roles) || ' entries for ' "
	    "|| COALESCE(len(children), 0) || ' children' "
	    "FROM all_objects WHERE children_roles IS NOT NULL "
	    "AND len(children_roles) <> COALESCE(len(children), 0)",

	    "SELECT 'id_duplicate', 'error', ANY_VALUE(__tbl), id, "
	    "'id appears ' || COUNT(*) || ' times in the package' "
	    "FROM all_objects GROUP BY id HAVING COUNT(*) > 1",
	};
}

std::string PragmaValidate(ClientContext &context, const FunctionParameters &parameters) {
	return BuildValidateSQL(context, parameters.values[0].ToString());
}

void ValidateSQLScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t schema) {
		return StringVector::AddString(result, BuildValidateSQL(context, schema.GetString()));
	});
}

} // namespace

std::string BuildValidateSQL(ClientContext &context, const std::string &schema) {
	auto object_tables = ObjectTablesInSchema(context, schema);

	std::string body = "WITH " + AllObjectsCTE(schema, object_tables) + "\n";
	body += StringUtil::Join(Checks(), "\nUNION ALL\n");

	// Materialise, then select: a PRAGMA cannot be a subquery, so handing back a bare
	// SELECT would leave the caller unable to filter the findings. A temp table gives
	// them rows now and a filterable relation afterwards.
	std::string sql = "CREATE OR REPLACE TEMP TABLE " + std::string(VALIDATION_TABLE) + " AS\n" + body + ";\n";
	sql += "SELECT * FROM " + std::string(VALIDATION_TABLE) + " ORDER BY severity, check_name, object_id;";
	return sql;
}

namespace {

std::string PragmaOrphans(ClientContext &context, const FunctionParameters &parameters) {
	return BuildOrphansSQL(context, parameters.values[0].ToString());
}

std::string PragmaVacuum(ClientContext &context, const FunctionParameters &parameters) {
	return BuildVacuumSQL(context, parameters.values[0].ToString());
}

void VacuumSQLScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t schema) {
		return StringVector::AddString(result, BuildVacuumSQL(context, schema.GetString()));
	});
}

} // namespace

void RegisterCityParquetValidateFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("cityparquet_validate", PragmaValidate, {LogicalType(LogicalTypeId::VARCHAR)}));
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("cityparquet_orphans", PragmaOrphans, {LogicalType(LogicalTypeId::VARCHAR)}));
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("cityparquet_vacuum", PragmaVacuum, {LogicalType(LogicalTypeId::VARCHAR)}));

	ScalarFunction validate_sql("cityparquet_validate_sql", {LogicalType(LogicalTypeId::VARCHAR)},
	                            LogicalType(LogicalTypeId::VARCHAR), ValidateSQLScalar);
	loader.RegisterFunction(validate_sql);

	ScalarFunction vacuum_sql("cityparquet_vacuum_sql", {LogicalType(LogicalTypeId::VARCHAR)},
	                          LogicalType(LogicalTypeId::VARCHAR), VacuumSQLScalar);
	loader.RegisterFunction(vacuum_sql);
}

} // namespace cityjson
} // namespace duckdb

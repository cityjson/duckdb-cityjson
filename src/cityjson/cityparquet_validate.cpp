#include "cityjson/cityparquet_validate.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "cityjson/cityparquet_sql_common.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#include <vector>

namespace duckdb {
namespace cityjson {

const char *const VALIDATION_TABLE = "cityparquet_validation";
const char *const ORPHAN_TABLE = "cityparquet_orphan_rows";

namespace {

//! True when `table.template.id` holds at least one real (non-NULL) value. Every
//! reader this extension ships writes `template` as an always-NULL reserved column
//! (spec 02-object-table-schema.mdx: present but unpopulated is not the same claim
//! as "nothing is referenced" -- see ReferencedIds below). Runs on a fresh internal
//! connection, like cityparquet_write.cpp's `Run` helper, so it only sees committed
//! state -- acceptable here because a pragma's generated SQL is itself expanded
//! from pre-batch state (TRAPS.md, "Generated SQL and the pragma layer").
bool HasNonNullTemplateReference(ClientContext &context, const std::string &schema, const std::string &table) {
	Connection connection(DatabaseInstance::GetDatabase(context));
	auto result = connection.Query("SELECT COUNT(*) FROM " + QualifiedName(schema, table) +
	                               " WHERE template IS NOT NULL");
	if (result->HasError()) {
		// Conservative default: treat a probe failure as "no evidence", the same
		// outcome as the column not existing at all.
		return false;
	}
	return result->GetValue(0, 0).GetValue<int64_t>() > 0;
}

} // namespace

std::string ReferencedIds(ClientContext &context, const std::string &schema, const std::string &sidecar) {
	auto object_tables = ObjectTablesInSchema(context, schema);
	std::vector<std::string> terms;

	if (sidecar == "geometry_templates") {
		// A template reference is a plain struct field, no JSON involved. A table
		// without the `template` column at all (predates it) contributes no
		// references, and neither does one whose `template` column exists but has
		// never been populated with a real value -- every reader this extension
		// ships writes `template` as an always-NULL reserved column today (spec
		// 02-object-table-schema.mdx: the column is present regardless of whether
		// anything populates it). Without this second check, a WHERE template IS
		// NOT NULL term over an all-NULL column returns zero rows, which is
		// indistinguishable in SQL from "this table genuinely references nothing"
		// -- and `x NOT IN (<empty set>)` is true for every row, so
		// BuildVacuumSQL would delete every geometry_templates row in the
		// package. Only a table that can show at least one real reference
		// contributes a term; if none can, `terms` stays empty and the
		// "undeterminable" fallback below fires, exactly as it does for a
		// package with no `template` column anywhere.
		for (const auto &table : object_tables) {
			if (!HasColumn(context, schema, table, "template")) {
				continue;
			}
			if (!HasNonNullTemplateReference(context, schema, table)) {
				continue;
			}
			terms.push_back("SELECT template.id AS ref FROM " + QualifiedName(schema, table) +
			                " WHERE template IS NOT NULL");
		}
	} else {
		const std::string prefix = (sidecar == "materials") ? "material_lod" : "texture_lod";
		const std::string kind = (sidecar == "materials") ? "material" : "texture";

		// Object tables are not the only holders of appearance references:
		// geometry_templates.parquet carries its own material_lod*/texture_lod* columns,
		// so a material used only by a template would otherwise look unreferenced and be
		// vacuumed out from under the template that needs it.
		auto tables = object_tables;
		for (const auto &sidecar_table : SidecarTablesInSchema(context, schema)) {
			if (sidecar_table == "geometry_templates") {
				tables.push_back(sidecar_table);
			}
		}

		for (const auto &table : tables) {
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

	// No table contributed a term: either nothing in the package can hold a
	// reference to this sidecar at all (no `material_lod*`/`texture_lod*` column,
	// or -- for geometry_templates -- no `template` column), or (geometry_templates
	// only) every `template` column that exists has never been populated with a
	// real value. Either way that is "no information", not "nothing is
	// referenced": reading it the other way would make vacuum silently delete
	// every geometry template in every real package, since `x NOT IN
	// (<empty set>)` is true for every row. The empty string signals
	// "undeterminable"; callers skip the sidecar.
	if (terms.empty()) {
		return std::string();
	}
	return Join(terms, "\nUNION\n");
}

std::string BuildOrphansSQL(ClientContext &context, const std::string &schema) {
	auto sidecars = SidecarTablesInSchema(context, schema);

	std::vector<std::string> parts;
	for (const auto &sidecar : sidecars) {
		const auto referenced = ReferencedIds(context, schema, sidecar);
		if (referenced.empty()) {
			continue; // undeterminable — see ReferencedIds
		}
		parts.push_back("SELECT " + KeywordHelper::WriteQuoted(sidecar, '\'') +
		                " AS table_name, "
		                "CAST(s.id AS VARCHAR) AS id, 'unreferenced' AS reason FROM " +
		                QualifiedName(schema, sidecar) + " s WHERE s.id NOT IN (" + referenced + ")");
	}

	std::string body;
	if (parts.empty()) {
		body = "SELECT NULL::VARCHAR AS table_name, NULL::VARCHAR AS id, NULL::VARCHAR AS reason WHERE false";
	} else {
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
	// Snapshot every sidecar's referenced set *before* deleting from any of them.
	// Sidecars can reference each other -- geometry_templates carries its own
	// material_lod*/texture_lod* columns -- so computing each set lazily would make the
	// result depend on delete order: vacuuming templates first would orphan the very
	// materials those templates referenced, and the next statement would delete them.
	std::string snapshot;
	std::string deletes;
	std::string cleanup;
	for (const auto &sidecar : sidecars) {
		const auto referenced = ReferencedIds(context, schema, sidecar);
		if (referenced.empty()) {
			continue; // undeterminable — see ReferencedIds
		}
		const auto temp = "__cp_ref_" + sidecar;
		snapshot += "CREATE OR REPLACE TEMP TABLE " + temp + " AS " + referenced + ";\n";
		// NOT IN: vacuum removes what nothing references. Inverting this would delete
		// precisely the rows still in use.
		deletes +=
		    "DELETE FROM " + QualifiedName(schema, sidecar) + " WHERE id NOT IN (SELECT ref FROM " + temp + ");\n";
		cleanup += "DROP TABLE IF EXISTS " + temp + ";\n";
	}
	if (snapshot.empty()) {
		return "SELECT 1 WHERE false;";
	}
	return snapshot + deletes + cleanup;
}

namespace {

// Every check is written once against the `all_objects` union rather than once per
// module table. The columns these checks touch -- id, feature_id, parents, children,
// children_roles -- are common to every module; only attribute columns differ.
std::vector<std::string> Checks() {
	return {
	    // The three dangling checks below anti-join against `SELECT id ... WHERE id IS
	    // NOT NULL`. Without that filter, a single NULL id anywhere in the package puts
	    // NULL in the NOT IN set, so every comparison evaluates to UNKNOWN and all three
	    // checks silently report nothing -- on precisely the malformed package they
	    // exist to diagnose.
	    // Every entry in this list is one SQL statement spread over several adjacent
	    // literals; the concatenation is deliberate, and the commas that separate the
	    // entries are the ones ending each statement.
	    // NOLINTNEXTLINE(bugprone-suspicious-missing-comma)
	    "SELECT 'feature_id_null' AS check_name, 'error' AS severity, __tbl AS table_name, "
	    "id AS object_id, 'feature_id is NULL' AS message "
	    "FROM all_objects WHERE feature_id IS NULL",

	    "SELECT 'feature_id_dangling', 'error', __tbl, id, "
	    "'feature_id ' || feature_id || ' matches no object id in the package' "
	    "FROM all_objects WHERE feature_id IS NOT NULL "
	    "AND feature_id NOT IN (SELECT id FROM all_objects WHERE id IS NOT NULL)",

	    // A parent or child reference resolves by bare id across every module file, so
	    // these anti-joins must span the whole package, not one table.
	    "SELECT 'parent_dangling', 'error', __tbl, id, "
	    "'parent ' || p || ' matches no object id in the package' "
	    "FROM all_objects, UNNEST(parents) AS t(p) "
	    "WHERE p IS NOT NULL AND p NOT IN (SELECT id FROM all_objects WHERE id IS NOT NULL)",

	    "SELECT 'child_dangling', 'error', __tbl, id, "
	    "'child ' || c || ' matches no object id in the package' "
	    "FROM all_objects, UNNEST(children) AS t(c) "
	    "WHERE c IS NOT NULL AND c NOT IN (SELECT id FROM all_objects WHERE id IS NOT NULL)",

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

	std::string body = "WITH " + AllObjectsCTE(context, schema, object_tables) + "\n";
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

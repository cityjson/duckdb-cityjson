#include "cityjson/cityparquet_validate.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <vector>

namespace duckdb {
namespace cityjson {

const char *const VALIDATION_TABLE = "cityparquet_validation";

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

void RegisterCityParquetValidateFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("cityparquet_validate", PragmaValidate, {LogicalType(LogicalTypeId::VARCHAR)}));

	ScalarFunction validate_sql("cityparquet_validate_sql", {LogicalType(LogicalTypeId::VARCHAR)},
	                            LogicalType(LogicalTypeId::VARCHAR), ValidateSQLScalar);
	loader.RegisterFunction(validate_sql);
}

} // namespace cityjson
} // namespace duckdb

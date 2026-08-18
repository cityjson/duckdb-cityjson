#include "cityjson/cityparquet_delete.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "cityjson/cityparquet_reconcile.hpp"
#include "cityjson/cityparquet_sql_common.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/parser.hpp"

#include <algorithm>
#include <set>

namespace duckdb {
namespace cityjson {

namespace {

//! Every column name the predicate mentions, lower-cased.
std::set<std::string> ReferencedColumns(const std::string &predicate) {
	std::set<std::string> columns;
	duckdb::vector<unique_ptr<ParsedExpression>> expressions;
	try {
		expressions = Parser::ParseExpressionList(predicate);
	} catch (const std::exception &e) {
		throw BinderException("cityparquet_delete: cannot parse predicate '%s': %s", predicate, e.what());
	}

	std::function<void(const ParsedExpression &)> walk = [&](const ParsedExpression &expression) {
		if (expression.GetExpressionClass() == ExpressionClass::COLUMN_REF) {
			auto &column_ref = expression.Cast<ColumnRefExpression>();
			// GetColumnName() returns the *last* name in the chain, which for a struct
			// field reference like `bbox.max_z` is the field, not the column. Recording
			// only that would make the table look as though it lacked a top-level
			// `max_z` column and reject a predicate that binds perfectly well. Record
			// the whole chain; a table qualifies if any one name in it is a real column,
			// which covers both `bbox.max_z` and a table-qualified `t.col`.
			std::string chain;
			for (const auto &part : column_ref.column_names) {
				if (!chain.empty()) {
					chain += ".";
				}
				chain += StringUtil::Lower(part);
			}
			columns.insert(chain);
		}
		ParsedExpressionIterator::EnumerateChildren(expression, [&](const ParsedExpression &child) { walk(child); });
	};
	for (const auto &expression : expressions) {
		walk(*expression);
	}
	return columns;
}

std::set<std::string> ColumnNames(ClientContext &context, const std::string &schema, const std::string &table) {
	std::set<std::string> names;
	auto &catalog_entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, INVALID_CATALOG, schema, table);
	for (auto &column : catalog_entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		names.insert(StringUtil::Lower(column.Name()));
	}
	return names;
}

std::vector<std::string> TablesFromParameters(const FunctionParameters &parameters) {
	std::vector<std::string> tables;
	auto entry = parameters.named_parameters.find("tables");
	if (entry != parameters.named_parameters.end()) {
		for (const auto &value : ListValue::GetChildren(entry->second)) {
			tables.push_back(StringUtil::Lower(value.ToString()));
		}
	}
	return tables;
}

bool CascadeFromParameters(const FunctionParameters &parameters) {
	auto entry = parameters.named_parameters.find("cascade");
	if (entry == parameters.named_parameters.end()) {
		return true;
	}
	return BooleanValue::Get(entry->second);
}

std::string PragmaDelete(ClientContext &context, const FunctionParameters &parameters) {
	return BuildDeleteSQL(context, parameters.values[0].ToString(), parameters.values[1].ToString(),
	                      CascadeFromParameters(parameters), TablesFromParameters(parameters));
}

void DeleteSQLScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	BinaryExecutor::Execute<string_t, string_t, string_t>(
	    args.data[0], args.data[1], result, args.size(), [&](string_t schema, string_t predicate) {
		    return StringVector::AddString(
		        result, BuildDeleteSQL(context, schema.GetString(), predicate.GetString(), true, {}));
	    });
}

} // namespace

std::vector<std::string> TablesBindingPredicate(ClientContext &context, const std::string &schema,
                                                const std::string &predicate,
                                                const std::vector<std::string> &restrict_to) {
	auto object_tables = ObjectTablesInSchema(context, schema);
	if (!restrict_to.empty()) {
		std::vector<std::string> filtered;
		for (const auto &table : object_tables) {
			if (std::find(restrict_to.begin(), restrict_to.end(), table) != restrict_to.end()) {
				filtered.push_back(table);
			}
		}
		object_tables = std::move(filtered);
	}

	const auto referenced = ReferencedColumns(predicate);
	std::vector<std::string> binding;
	for (const auto &table : object_tables) {
		const auto available = ColumnNames(context, schema, table);
		bool all_present = true;
		for (const auto &chain : referenced) {
			// A reference is satisfied when any name in its dotted chain is a real
			// column of this table: `bbox.max_z` matches on `bbox`, `t.status` on
			// `status`, and a bare `status` on itself.
			bool matched = false;
			size_t start = 0;
			while (start <= chain.size()) {
				const auto dot = chain.find('.', start);
				const auto part = chain.substr(start, dot == std::string::npos ? std::string::npos : dot - start);
				if (available.count(part) > 0) {
					matched = true;
					break;
				}
				if (dot == std::string::npos) {
					break;
				}
				start = dot + 1;
			}
			if (!matched) {
				all_present = false;
				break;
			}
		}
		if (all_present) {
			binding.push_back(table);
		}
	}
	return binding;
}

std::string BuildDeleteSQL(ClientContext &context, const std::string &schema, const std::string &predicate,
                           bool cascade, const std::vector<std::string> &tables) {
	auto object_tables = ObjectTablesInSchema(context, schema);
	auto matching_tables = TablesBindingPredicate(context, schema, predicate, tables);
	if (matching_tables.empty()) {
		throw BinderException("cityparquet_delete: no object table in schema '%s' carries every column the "
		                      "predicate references",
		                      schema);
	}

	// The node/edge temp tables the closure walks, shared with reconcile.
	std::string sql = BuildReconcilePrelude(context, schema);

	std::vector<std::string> seeds;
	seeds.reserve(matching_tables.size());
	for (const auto &table : matching_tables) {
		seeds.push_back("SELECT id FROM " + QualifiedName(schema, table) + " WHERE " + predicate);
	}
	const auto seed_sql = Join(seeds, "\nUNION\n");

	if (cascade) {
		// Grow through `children`, never through `feature_id` equality. A predicate may
		// match a non-root object, and every member of a feature family shares its
		// feature_id -- so equality would delete a matched BuildingPart's parent
		// Building and all its siblings. UNION (not UNION ALL) terminates on cycles.
		sql += "CREATE OR REPLACE TEMP TABLE __cp_deleted AS\n"
		       "WITH RECURSIVE seed AS (\n" +
		       seed_sql +
		       "\n),\n"
		       "closure(id) AS (\n"
		       "  SELECT id FROM seed\n"
		       "  UNION\n"
		       "  SELECT e.child FROM closure c JOIN __cp_edges e ON e.parent = c.id\n"
		       ")\n"
		       "SELECT id FROM closure;\n";
	} else {
		sql += "CREATE OR REPLACE TEMP TABLE __cp_deleted AS\n" + seed_sql + ";\n";
	}

	for (const auto &table : object_tables) {
		sql += "DELETE FROM " + QualifiedName(schema, table) + " WHERE id IN (SELECT id FROM __cp_deleted);\n";
	}

	// Strip the deleted ids out of every survivor's hierarchy arrays. children_roles is
	// positionally aligned to children, so the two are filtered together through a
	// zipped list rather than independently -- filtering them separately would silently
	// misalign the roles that remain.
	//
	// The deleted set is hoisted into a session variable because DuckDB rejects
	// subqueries inside lambda bodies ("subqueries in lambda expressions are not
	// supported"), and list_filter needs a lambda.
	sql += "SET VARIABLE __cp_deleted_ids = (SELECT COALESCE(list(id), []) FROM __cp_deleted);\n";

	for (const auto &table : object_tables) {
		const auto qualified = QualifiedName(schema, table);
		sql += "UPDATE " + qualified +
		       " t SET children = list_transform(f.kept, x -> x[1]), "
		       "children_roles = CASE WHEN t.children_roles IS NULL THEN NULL "
		       "ELSE list_transform(f.kept, x -> x[2]) END "
		       "FROM (SELECT id, list_filter(list_zip(children, children_roles), "
		       "x -> NOT list_contains(getvariable('__cp_deleted_ids'), x[1])) AS kept FROM " +
		       qualified + " WHERE children IS NOT NULL) f WHERE t.id = f.id;\n";
		sql += "UPDATE " + qualified +
		       " SET parents = list_filter(parents, x -> NOT list_contains(getvariable('__cp_deleted_ids'), x)) "
		       "WHERE parents IS NOT NULL;\n";
		// Normalise emptied lists to NULL, so "root" stays a single test everywhere.
		sql += "UPDATE " + qualified +
		       " SET children = NULL, children_roles = NULL WHERE children IS NOT NULL AND len(children) = 0;\n";
		sql += "UPDATE " + qualified + " SET parents = NULL WHERE parents IS NOT NULL AND len(parents) = 0;\n";
	}

	sql += "DROP TABLE IF EXISTS __cp_deleted;\n";

	// feature_id and bbox both go stale. feature_id is not optional even under
	// cascade = false: an orphaned descendant has just become a root, but still names
	// the deleted object as its feature family.
	sql += BuildReconcileSQL(context, schema, {"feature_id", "bbox"});
	return sql;
}

void RegisterCityParquetDeleteFunctions(ExtensionLoader &loader) {
	auto pragma = PragmaFunction::PragmaCall(
	    "cityparquet_delete", PragmaDelete, {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)});
	pragma.named_parameters["cascade"] = LogicalType(LogicalTypeId::BOOLEAN);
	pragma.named_parameters["tables"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
	loader.RegisterFunction(pragma);

	ScalarFunction delete_sql("cityparquet_delete_sql",
	                          {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                          LogicalType(LogicalTypeId::VARCHAR), DeleteSQLScalar);
	loader.RegisterFunction(delete_sql);
}

} // namespace cityjson
} // namespace duckdb

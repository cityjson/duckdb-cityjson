#include "cityjson/cityparquet_reconcile.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <algorithm>
#include <set>

namespace duckdb {
namespace cityjson {

namespace {

//! StringUtil::Join takes duckdb::vector, which std::vector does not convert to.
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

bool Wants(const std::vector<std::string> &checks, const char *name) {
	return checks.empty() || std::find(checks.begin(), checks.end(), std::string(name)) != checks.end();
}

//! Every object id in the package, and the child->parent edges between them. Both are
//! plain temp tables so the recursive walks below can run over them directly -- a
//! recursive CTE cannot recurse through UNNEST of a list column.
std::string NodesAndEdges(const std::string &schema, const std::vector<std::string> &object_tables) {
	const auto cte = "WITH " + AllObjectsCTE(schema, object_tables) + "\n";
	std::string sql;
	sql += "CREATE OR REPLACE TEMP TABLE __cp_nodes AS\n" + cte + "SELECT id FROM all_objects;\n";
	sql += "CREATE OR REPLACE TEMP TABLE __cp_edges AS\n" + cte +
	       "SELECT id AS child, p AS parent FROM all_objects, UNNEST(parents) AS u(p) WHERE p IS NOT NULL;\n";
	return sql;
}

//! feature_id = the id of the object's root parent (itself, for a root).
std::string FeatureIdPhase(const std::string &schema, const std::vector<std::string> &object_tables) {
	std::string sql;
	// Walk each node up to the top of its parent chain. A node with no outgoing edge is
	// its own root; the recursion keeps only the deepest reachable ancestor per node.
	sql += "CREATE OR REPLACE TEMP TABLE __cp_roots AS\n"
	       "WITH RECURSIVE up(node, ancestor) AS (\n"
	       "  SELECT id, id FROM __cp_nodes\n"
	       "  UNION\n"
	       "  SELECT u.node, e.parent FROM up u JOIN __cp_edges e ON e.child = u.ancestor\n"
	       ")\n"
	       "SELECT node AS id, ancestor AS root FROM up\n"
	       "WHERE ancestor NOT IN (SELECT child FROM __cp_edges);\n";

	for (const auto &table : object_tables) {
		sql += "UPDATE " + QualifiedName(schema, table) + " t SET feature_id = r.root " + "FROM __cp_roots r WHERE r.id = t.id AND t.feature_id IS DISTINCT FROM r.root;\n";
	}
	return sql;
}

//! children/children_roles rebuilt from the parents arrays that actually point at each row.
std::string HierarchyPhase(const std::string &schema, const std::vector<std::string> &object_tables) {
	std::string sql;
	sql += "CREATE OR REPLACE TEMP TABLE __cp_kids AS\n"
	       "SELECT parent AS parent_id, list(child ORDER BY child) AS kids FROM __cp_edges GROUP BY parent;\n";

	for (const auto &table : object_tables) {
		const auto qualified = QualifiedName(schema, table);
		// Rewrite only where the child *set* is genuinely wrong. Comparing sorted lists
		// rather than the lists themselves leaves a correct-but-differently-ordered
		// children array alone, which matters because children_roles is positionally
		// aligned to it and a needless reorder would silently invalidate the roles.
		sql += "UPDATE " + qualified +
		       " t SET children = k.kids, children_roles = NULL "
		       "FROM __cp_kids k WHERE k.parent_id = t.id "
		       "AND list_sort(COALESCE(t.children, [])) IS DISTINCT FROM list_sort(k.kids);\n";
		// A row nothing claims as a parent has no children at all.
		sql += "UPDATE " + qualified +
		       " t SET children = NULL, children_roles = NULL "
		       "WHERE t.children IS NOT NULL "
		       "AND NOT EXISTS (SELECT 1 FROM __cp_kids k WHERE k.parent_id = t.id);\n";
	}
	return sql;
}

//! bbox = the object's own geometry extent, unioned across every stored LoD *and*
//! across all of its descendants.
std::string BboxPhase(ClientContext &context, const std::string &schema,
                      const std::vector<std::string> &object_tables) {
	std::string sql;

	// Own extent, per row: LEAST/GREATEST across every geometry_lod* column the table
	// happens to have. LEAST and GREATEST ignore NULLs, so a row with geometry at only
	// some LoDs still yields the extent of the ones it has, and a row with none yields
	// NULL throughout.
	std::vector<std::string> own_parts;
	for (const auto &table : object_tables) {
		auto geometry_columns = GeometryLodColumns(context, schema, table);
		if (geometry_columns.empty()) {
			// A table with no analysis geometry contributes ids but no extents, so its
			// rows can still receive a bbox unioned from descendants elsewhere.
			own_parts.push_back("SELECT id, NULL::DOUBLE AS min_x, NULL::DOUBLE AS min_y, NULL::DOUBLE AS min_z, "
			                    "NULL::DOUBLE AS max_x, NULL::DOUBLE AS max_y, NULL::DOUBLE AS max_z FROM " +
			                    QualifiedName(schema, table));
			continue;
		}
		std::vector<std::string> extents;
		for (const auto &column : geometry_columns) {
			extents.push_back("cityjson_wkb_extent(" + KeywordHelper::WriteOptionallyQuoted(column) + ")");
		}
		auto agg = [&](const char *fn, const char *field) {
			std::vector<std::string> terms;
			for (const auto &extent : extents) {
				terms.push_back(extent + "." + field);
			}
			return std::string(fn) + "(" + Join(terms, ", ") + ") AS " + field;
		};
		own_parts.push_back("SELECT id, " + agg("LEAST", "min_x") + ", " + agg("LEAST", "min_y") + ", " +
		                    agg("LEAST", "min_z") + ", " + agg("GREATEST", "max_x") + ", " +
		                    agg("GREATEST", "max_y") + ", " + agg("GREATEST", "max_z") + " FROM " +
		                    QualifiedName(schema, table));
	}
	sql += "CREATE OR REPLACE TEMP TABLE __cp_own AS\n" + Join(own_parts, "\nUNION ALL\n") + ";\n";

	// Every (node, ancestor) pair, each node included as its own ancestor, so grouping
	// by ancestor unions a subtree's extents into the object at its root.
	sql += "CREATE OR REPLACE TEMP TABLE __cp_anc AS\n"
	       "WITH RECURSIVE anc(node, ancestor) AS (\n"
	       "  SELECT id, id FROM __cp_nodes\n"
	       "  UNION\n"
	       "  SELECT a.node, e.parent FROM anc a JOIN __cp_edges e ON e.child = a.ancestor\n"
	       ")\n"
	       "SELECT node, ancestor FROM anc;\n";

	sql += "CREATE OR REPLACE TEMP TABLE __cp_bbox AS\n"
	       "SELECT a.ancestor AS id, MIN(o.min_x) AS min_x, MIN(o.min_y) AS min_y, MIN(o.min_z) AS min_z, "
	       "MAX(o.max_x) AS max_x, MAX(o.max_y) AS max_y, MAX(o.max_z) AS max_z "
	       "FROM __cp_anc a JOIN __cp_own o ON o.id = a.node GROUP BY a.ancestor;\n";

	for (const auto &table : object_tables) {
		// An object with neither geometry of its own nor any descendant carrying
		// geometry gets NULL, not a degenerate zero box.
		sql += "UPDATE " + QualifiedName(schema, table) +
		       " t SET bbox = CASE WHEN b.min_x IS NULL THEN NULL ELSE "
		       "{'min_x': b.min_x, 'min_y': b.min_y, 'min_z': b.min_z, "
		       "'max_x': b.max_x, 'max_y': b.max_y, 'max_z': b.max_z} END "
		       "FROM __cp_bbox b WHERE b.id = t.id;\n";
	}
	return sql;
}

std::string DropTemps() {
	return "DROP TABLE IF EXISTS __cp_roots;\n"
	       "DROP TABLE IF EXISTS __cp_kids;\n"
	       "DROP TABLE IF EXISTS __cp_own;\n"
	       "DROP TABLE IF EXISTS __cp_anc;\n"
	       "DROP TABLE IF EXISTS __cp_bbox;\n"
	       "DROP TABLE IF EXISTS __cp_edges;\n"
	       "DROP TABLE IF EXISTS __cp_nodes;\n";
}

std::vector<std::string> ChecksFromParameters(const FunctionParameters &parameters) {
	std::vector<std::string> checks;
	auto entry = parameters.named_parameters.find("checks");
	if (entry != parameters.named_parameters.end()) {
		for (const auto &value : ListValue::GetChildren(entry->second)) {
			checks.push_back(StringUtil::Lower(value.ToString()));
		}
	}
	return checks;
}

std::string PragmaReconcile(ClientContext &context, const FunctionParameters &parameters) {
	return BuildReconcileSQL(context, parameters.values[0].ToString(), ChecksFromParameters(parameters));
}

void ReconcileSQLScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t schema) {
		return StringVector::AddString(result, BuildReconcileSQL(context, schema.GetString(), {}));
	});
}

} // namespace

std::string BuildReconcilePrelude(ClientContext &context, const std::string &schema) {
	return NodesAndEdges(schema, ObjectTablesInSchema(context, schema));
}

std::string BuildReconcileSQL(ClientContext &context, const std::string &schema,
                              const std::vector<std::string> &checks) {
	auto object_tables = ObjectTablesInSchema(context, schema);

	static const std::set<std::string> known = {"feature_id", "hierarchy", "bbox"};
	for (const auto &check : checks) {
		if (known.count(check) == 0) {
			throw BinderException("cityparquet_reconcile: unknown check '%s' "
			                      "(expected feature_id, hierarchy or bbox)",
			                      check);
		}
	}

	std::string sql = NodesAndEdges(schema, object_tables);
	// Order is normative: hierarchy corrects the parent chain that feature_id and bbox
	// both read, and bbox unions across descendants so it must come last.
	if (Wants(checks, "hierarchy")) {
		sql += HierarchyPhase(schema, object_tables);
	}
	if (Wants(checks, "feature_id")) {
		sql += FeatureIdPhase(schema, object_tables);
	}
	if (Wants(checks, "bbox")) {
		sql += BboxPhase(context, schema, object_tables);
	}
	sql += DropTemps();
	return sql;
}

void RegisterCityParquetReconcileFunctions(ExtensionLoader &loader) {
	auto pragma =
	    PragmaFunction::PragmaCall("cityparquet_reconcile", PragmaReconcile, {LogicalType(LogicalTypeId::VARCHAR)});
	pragma.named_parameters["checks"] = LogicalType::LIST(LogicalType(LogicalTypeId::VARCHAR));
	loader.RegisterFunction(pragma);

	ScalarFunction reconcile_sql("cityparquet_reconcile_sql", {LogicalType(LogicalTypeId::VARCHAR)},
	                             LogicalType(LogicalTypeId::VARCHAR), ReconcileSQLScalar);
	loader.RegisterFunction(reconcile_sql);
}

} // namespace cityjson
} // namespace duckdb

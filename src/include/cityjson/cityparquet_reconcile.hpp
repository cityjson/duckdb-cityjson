#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <map>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * Module tables that will exist by the time the generated script runs, but do not exist
 * yet at plan time — the ones a merge or an insert creates itself.
 *
 * A pragma's generator sees the catalog as it was before the batch, so without this a
 * newly created table takes no part in the reconcile that follows in the same script:
 * its rows never enter the node set, and a destination row that has just become their
 * parent never gets the reciprocal `children` entry.
 *
 * Carries what the bbox phase would otherwise read from the catalog.
 */
struct PendingTable {
	//! The `geometry_lod*` columns the table is being created with.
	std::vector<std::string> geometry_columns;
	//! Whether it has a `bbox` column at all. A source with no analysis geometry produces
	//! no geometry columns and no bbox column either, and a bbox UPDATE against such a
	//! table is a binder error rather than a no-op.
	bool has_bbox = false;
	//! Likewise for `children_roles`, which a table read in `lod =` mode does not carry.
	bool has_children_roles = true;
};

using PendingTables = std::map<std::string, PendingTable>;

/**
 * Re-derives the state a raw SQL edit invalidates: `feature_id`, the reciprocal
 * `parents`/`children` arrays, and `bbox`.
 *
 * The emitted order is normative and must not be rearranged. `bbox` is unioned across
 * an object's descendants, so it depends on the hierarchy having been corrected first;
 * and the hierarchy fix-up is what makes the root-parent chain `feature_id` walks
 * trustworthy.
 */

//! `checks` selects from {"feature_id", "hierarchy", "bbox"}; empty means all three.
//! Regardless of the order given, the script emits feature_id, then hierarchy, then bbox.
std::string BuildReconcileSQL(ClientContext &context, const std::string &schema, const std::vector<std::string> &checks,
                              const PendingTables &pending = {});

//! The temp tables the reconcile script builds and drops. Exposed so cityparquet_delete
//! can reuse the same derivation without duplicating it.
std::string BuildReconcilePrelude(ClientContext &context, const std::string &schema);

void RegisterCityParquetReconcileFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

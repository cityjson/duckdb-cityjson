#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

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
std::string BuildReconcileSQL(ClientContext &context, const std::string &schema,
                              const std::vector<std::string> &checks);

//! The temp tables the reconcile script builds and drops. Exposed so cityparquet_delete
//! can reuse the same derivation without duplicating it.
std::string BuildReconcilePrelude(ClientContext &context, const std::string &schema);

void RegisterCityParquetReconcileFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

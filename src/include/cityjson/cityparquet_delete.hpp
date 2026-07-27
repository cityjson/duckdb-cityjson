#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * Deletes city objects matching a predicate, with the consistency work that makes the
 * package survive it: the transitive `children` closure, stripping the deleted ids out
 * of surviving rows' hierarchy arrays, and re-deriving `feature_id` and `bbox`.
 */

//! Object tables in `schema` against which `predicate` can bind — those carrying every
//! column it references. Object tables share only their structural columns, so applying
//! an attribute predicate to all of them would fail to bind against most.
std::vector<std::string> TablesBindingPredicate(ClientContext &context, const std::string &schema,
                                                const std::string &predicate,
                                                const std::vector<std::string> &restrict_to);

std::string BuildDeleteSQL(ClientContext &context, const std::string &schema, const std::string &predicate,
                           bool cascade, const std::vector<std::string> &tables);

void RegisterCityParquetDeleteFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

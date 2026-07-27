#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * Merges one CityParquet package (a DuckDB schema) into another, preserving every
 * relationship the package depends on: object ids stay unique, sidecar references are
 * renumbered onto the destination's numbering, the destination's schema grows to
 * accommodate LoDs and attributes it lacked, and the derived state (`feature_id`,
 * hierarchy, `bbox`) is re-derived afterwards.
 *
 * The phase order is normative — see the design document. In particular, schema
 * evolution must complete before any INSERT, and the sidecar offset must be computed
 * from row data by the generated SQL rather than at plan time.
 */
struct MergeOptions {
	bool create_tables = true;
	std::vector<std::string> tables; // empty = every module table
};

std::string BuildMergeSQL(ClientContext &context, const std::string &destination, const std::string &source,
                          const MergeOptions &options);

void RegisterCityParquetMergeFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

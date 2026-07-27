#pragma once

#include "duckdb.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <optional>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * `insert_cityjson(schema, path)` — one call to add a CityJSON file to a CityParquet
 * package held in a DuckDB schema, transactionally and consistently.
 *
 * Doing this by hand is a page of SQL that has to be got right in a particular order:
 * route each object to its CityGML module table, create the tables and sidecars the
 * source needs, renumber the incoming material/texture/template ids so they do not
 * collide with the ones already there, rewrite every reference in the object rows to
 * match, and re-derive `feature_id`, the reciprocal hierarchy and `bbox` afterwards.
 * Getting the order wrong is silent: references end up pointing at another file's
 * definitions.
 *
 * Like the rest of this layer it is a SQL-generating pragma, so DuckDB runs the script
 * inside the caller's transaction and atomicity is DuckDB's rather than ours.
 *
 * **The file is opened twice** — once at plan time to learn the schema and the object
 * types, once by the generated `read_cityjson` call — and the plan-time pass reads it
 * whole, because a sample cannot tell you that a rare type appears only in the tail.
 */

struct InsertOptions {
	//! Create module tables and sidecars the source needs and the destination lacks.
	bool create_tables = true;
	//! Restrict the insert to these module tables; empty means all of them.
	std::vector<std::string> tables;
	//! Passed through to the reader.
	std::optional<std::string> target_lod;
	size_t sample_lines = 100;
};

//! `reader_function` is the read function the generated SQL should call — this is what
//! makes insert_cityjson / insert_cityjsonseq / insert_flatcitybuf differ. The plan-time
//! inspection uses the matching reader, so the columns it derives are exactly the ones
//! that read will produce.
std::string BuildInsertSQL(ClientContext &context, const std::string &schema, const std::string &path,
                           const std::string &reader_function, const InsertOptions &options);

void RegisterCityParquetInsertFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

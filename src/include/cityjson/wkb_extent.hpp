#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

/**
 * The DuckDB type of a 3D extent:
 * STRUCT(min_x, min_y, min_z, max_x, max_y, max_z DOUBLE).
 *
 * Field names deliberately match the `bbox` column built in column_types.cpp, so a
 * recomputed extent can be assigned straight into `bbox` by generated SQL. Note the
 * CityParquet specification writes these as xmin/ymin/zmin/xmax/ymax/zmax — the
 * divergence is pre-existing and tracked in docs/CITYPARQUET_SPEC_QUESTIONS.md.
 */
LogicalType ExtentType();

/**
 * Registers cityjson_wkb_extent(BLOB) -> STRUCT(min_x, ..., max_z).
 *
 * DuckDB spatial cannot serve this purpose: it rejects PolyhedralSurfaceZ, which is
 * what every CityParquet solid LoD is encoded as.
 */
void RegisterWKBExtentFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

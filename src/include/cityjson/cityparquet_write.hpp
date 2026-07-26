#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

/**
 * Writes a CityParquet package held in a DuckDB schema back out as a directory of
 * Parquet files, with regenerated footers and a STAC Item.
 *
 * A **table function**, unlike the rest of this layer, and the reason is specific:
 * `KV_METADATA` cannot omit a *key*. A NULL value writes the literal string "NULL", and
 * the specification requires a table whose geometry is entirely solid to write **no**
 * `geo` key at all — which is the normal 3DBAG shape, not an edge case. Legality depends
 * on the data, so it cannot be settled at plan time, and SQL cannot branch the shape of
 * a COPY statement. So the metadata is assembled in C++ and the COPY is issued with only
 * the keys that belong.
 *
 * The cost is that it runs on an internal connection and therefore sees **committed**
 * state: mutate, commit, then write.
 *
 *   SELECT * FROM cityparquet_write('ams', 'amsterdam/', crs => 'EPSG:7415');
 *   -- (file, action, rows, bytes)
 */
void RegisterCityParquetWriteFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

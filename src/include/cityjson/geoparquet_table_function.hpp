#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

// Registers `cityjson_geoparquet_geo(path)` — a table function returning a single
// `geo` VARCHAR: the GeoParquet 1.1 `geo` metadata JSON for the dataset, ready to
// pass to COPY … TO (FORMAT PARQUET, KV_METADATA {geo: …}). Declares only
// GeoParquet-legal geometry columns (CityParquet spec §13.3) with PROJJSON CRS
// from the embedded EPSG table; `geo` is NULL when no column qualifies.
void RegisterGeoParquetTableFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

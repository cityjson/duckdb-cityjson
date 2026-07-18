#pragma once
namespace duckdb {
namespace cityjson {
// Gzip-compressed JSON object mapping EPSG code (as a decimal-string key) to its
// PROJJSON definition. Decompressed and parsed on first use (see crs_projjson.cpp).
extern const unsigned char EPSG_PROJJSON_GZ[];
extern const unsigned int EPSG_PROJJSON_GZ_LEN;
} // namespace cityjson
} // namespace duckdb

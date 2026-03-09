#pragma once

#ifdef CITYJSON_HAS_FCB

namespace duckdb {

class ExtensionLoader;

namespace cityjson {

/**
 * Register read_flatcitybuf table function
 */
void RegisterFlatCityBufTableFunction(ExtensionLoader &loader);

/**
 * Register flatcitybuf_metadata table function
 */
void RegisterFlatCityBufMetadataTableFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB

#define DUCKDB_EXTENSION_MAIN

#include "cityjson_extension.hpp"
#include "cityjson/table_function.hpp"
#include "cityjson/metadata_table_function.hpp"
#include "cityjson/geoparquet_table_function.hpp"
#include "cityjson/wkb_extent.hpp"
#include "cityjson/cityparquet_package.hpp"
#include "cityjson/cityparquet_validate.hpp"
#include "cityjson/cityparquet_reconcile.hpp"
#include "cityjson/cityparquet_delete.hpp"
#include "cityjson/cityparquet_merge.hpp"
#include "cityjson/cityparquet_appearance.hpp"
#include "cityjson/appearance_table_function.hpp"
#include "cityjson/copy_function.hpp"
#ifdef CITYJSON_HAS_FCB
#include "cityjson/flatcitybuf_table_function.hpp"
#endif
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the read_cityjson table function
	cityjson::RegisterCityJSONTableFunction(loader);

	// Register the read_cityjsonseq table function (dedicated CityJSONSeq reader)
	cityjson::RegisterCityJSONSeqTableFunction(loader);

	// Register the cityjson_metadata table function
	cityjson::RegisterMetadataTableFunction(loader);

	// Register the cityjsonseq_metadata table function (dedicated CityJSONSeq metadata reader)
	cityjson::RegisterCityJSONSeqMetadataTableFunction(loader);

	// Register cityjson_geoparquet_geo (GeoParquet `geo` metadata JSON for KV_METADATA)
	cityjson::RegisterGeoParquetTableFunctions(loader);

	// Register cityjson_wkb_extent (3D extent of a WKB blob, solid family included)
	cityjson::RegisterWKBExtentFunction(loader);

	// Register CityParquet package bookkeeping (cityparquet_init)
	cityjson::RegisterCityParquetPackageFunctions(loader);

	// Register CityParquet consistency checks (cityparquet_validate)
	cityjson::RegisterCityParquetValidateFunctions(loader);

	// Register CityParquet derived-state re-derivation (cityparquet_reconcile)
	cityjson::RegisterCityParquetReconcileFunctions(loader);

	// Register CityParquet delete with cascade (cityparquet_delete)
	cityjson::RegisterCityParquetDeleteFunctions(loader);

	// Register CityParquet package-to-package merge (cityparquet_merge)
	cityjson::RegisterCityParquetMergeFunctions(loader);

	// Register cityjson_appearance_ids (sidecar ids referenced by an appearance cell)
	cityjson::RegisterAppearanceIdsFunction(loader);

	// Register the CityParquet appearance sidecar readers
	cityjson::RegisterAppearanceTableFunctions(loader);

	// Register COPY TO functions (cityjson and cityjsonseq formats)
	cityjson::RegisterCityJSONCopyFunction(loader);
	cityjson::RegisterCityJSONSeqCopyFunction(loader);

#ifdef CITYJSON_HAS_FCB
	// Register FlatCityBuf functions
	cityjson::RegisterFlatCityBufTableFunction(loader);
	cityjson::RegisterFlatCityBufMetadataTableFunction(loader);
	cityjson::RegisterFlatCityBufCopyFunction(loader);
#endif
}

void CityjsonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string CityjsonExtension::Name() {
	return "cityjson";
}

std::string CityjsonExtension::Version() const {
#ifdef EXT_VERSION_CITYJSON
	return EXT_VERSION_CITYJSON;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(cityjson, loader) {
	duckdb::LoadInternal(loader);
}
}

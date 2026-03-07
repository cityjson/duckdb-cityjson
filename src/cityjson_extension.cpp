#define DUCKDB_EXTENSION_MAIN

#include "cityjson_extension.hpp"
#include "cityjson/table_function.hpp"
#include "cityjson/metadata_table_function.hpp"
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

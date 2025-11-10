#define DUCKDB_EXTENSION_MAIN

#include "cityjson_extension.hpp"
#include "cityjson/table_function.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register the read_cityjson table function
	cityjson::RegisterCityJSONTableFunction(*loader.GetDatabase().instance);
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

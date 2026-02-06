#include "cityjson/table_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

TableFunction CreateReadCityJSONTableFunction() {
	TableFunction func("read_cityjson", {LogicalType::VARCHAR}, CityJSONScan, CityJSONBind);

	// Named parameters
	func.named_parameters["sample_lines"] = LogicalType::BIGINT;

	// Set callbacks
	func.init_global = CityJSONInitGlobal;
	func.init_local = CityJSONInitLocal;
	func.cardinality = CityJSONCardinality;
	func.statistics = CityJSONStatistics;

	// Enable projection pushdown
	func.projection_pushdown = true;

	return func;
}

void RegisterCityJSONTableFunction(ExtensionLoader &loader) {
	auto func = CreateReadCityJSONTableFunction();
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

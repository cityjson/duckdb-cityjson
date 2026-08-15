#include "cityjson/table_function.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

TableFunction CreateReadCityJSONTableFunction() {
	TableFunction func("read_cityjson", {LogicalType::VARCHAR}, CityJSONScan, CityJSONBind);

	// Named parameters
	func.named_parameters["sample_lines"] = LogicalType::BIGINT;
	func.named_parameters["lod"] = LogicalType::VARCHAR; // Filter to specific LOD (e.g., "2.2")
	func.named_parameters["appearance"] = LogicalType::VARCHAR; // 'local' (default) or 'sidecar'
	// 'wkb' (default) or 'arrow-native' -- experimental, see GeometryEncoding.
	func.named_parameters["geometry_encoding"] = LogicalType::VARCHAR;

	// Set callbacks
	func.init_global = CityJSONInitGlobal;
	func.init_local = CityJSONInitLocal;
	func.cardinality = CityJSONCardinality;
	func.statistics = CityJSONStatistics;
	// Progress for the DuckDB progress bar. Materialised scans report the per cent
	// of batches done; streaming reports -1 (unknown). Not registered for
	// read_flatcitybuf: its chunks live in the global state, where this callback
	// would divide by an empty bind-data chunk count and claim 100%.
	func.table_scan_progress = CityJSONProgress;

	// Enable projection pushdown and complex-filter pushdown.
	// filter_pushdown is intentionally disabled; our scan callback does not
	// implement TableFilterSet handling, and enabling it would cause DuckDB to
	// drop filters it believes the scan will apply.
	func.projection_pushdown = true;
	func.filter_pushdown = false;
	func.pushdown_complex_filter = CityJSONPushdownComplexFilter;

	return func;
}

void RegisterCityJSONTableFunction(ExtensionLoader &loader) {
	auto func = CreateReadCityJSONTableFunction();
	loader.RegisterFunction(func);
}

TableFunction CreateReadCityJSONSeqTableFunction() {
	TableFunction func("read_cityjsonseq", {LogicalType::VARCHAR}, CityJSONScan, CityJSONSeqBind);

	// Named parameters (same as read_cityjson)
	func.named_parameters["sample_lines"] = LogicalType::BIGINT;
	func.named_parameters["lod"] = LogicalType::VARCHAR; // Filter to specific LOD (e.g., "2.2")
	func.named_parameters["appearance"] = LogicalType::VARCHAR; // 'local' (default) or 'sidecar'
	// 'wkb' (default) or 'arrow-native' -- experimental, see GeometryEncoding.
	func.named_parameters["geometry_encoding"] = LogicalType::VARCHAR;

	// Set callbacks (reuse same stateless callbacks)
	func.init_global = CityJSONInitGlobal;
	func.init_local = CityJSONInitLocal;
	func.cardinality = CityJSONCardinality;
	func.statistics = CityJSONStatistics;
	// See CreateReadCityJSONTableFunction for why this is registered here but not
	// on read_flatcitybuf.
	func.table_scan_progress = CityJSONProgress;

	// Enable projection pushdown and complex-filter pushdown.
	func.projection_pushdown = true;
	func.filter_pushdown = false;
	func.pushdown_complex_filter = CityJSONPushdownComplexFilter;

	return func;
}

void RegisterCityJSONSeqTableFunction(ExtensionLoader &loader) {
	auto func = CreateReadCityJSONSeqTableFunction();
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

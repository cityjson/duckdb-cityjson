#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<NodeStatistics> CityJSONCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

	// Streaming scans do not materialise chunks during bind, so `chunks` is empty and
	// would otherwise advertise ~0 rows to the optimiser. Report unknown cardinality
	// instead of a misleading zero estimate.
	if (bind_data.streaming) {
		return nullptr;
	}

	// Count total CityObjects
	size_t total = bind_data.chunks.TotalCityObjectCount();

	auto stats = make_uniq<NodeStatistics>();
	stats->has_estimated_cardinality = true;
	stats->estimated_cardinality = total;
	stats->has_max_cardinality = true;
	stats->max_cardinality = total;

	return stats;
}

double CityJSONProgress(ClientContext &context, const FunctionData *bind_data_p,
                        const GlobalTableFunctionState *global_state_p) {
	auto &bind_data = bind_data_p->Cast<CityJSONBindData>();
	auto &global_state = global_state_p->Cast<CityJSONGlobalState>();

	// Streaming scans have no materialised total to divide against; deriving progress
	// from the empty `chunks` would report 100% immediately. Return -1 (unknown) so
	// DuckDB does not display a bogus progress value.
	if (bind_data.streaming) {
		return -1.0;
	}

	// With pushed-down equality filters the scan walks the shared, non-atomic cursor
	// (filter_chunk_idx / filter_feature_idx) rather than the batch plan, so batch_index
	// never advances and this would report a frozen 0%. Reading the cursor instead would
	// race with the scan threads, so report unknown, as the streaming path does.
	if (!bind_data.equality_filters.empty()) {
		return -1.0;
	}

	// The contract is a percentage, not a fraction: PhysicalTableScan::GetProgress takes
	// this straight as `res.done` against a fixed `res.total = 100.0`
	// (duckdb/src/execution/operator/scan/physical_table_scan.cpp:238-243), and DuckDB's
	// own table scan returns 100 * scanned/total clamped to 100
	// (duckdb/src/function/table/table_scan.cpp:242-249).
	size_t total = bind_data.chunks.TotalCityObjectCount();
	if (total == 0) {
		return 100.0;
	}

	size_t processed = global_state.batch_index.load() * STANDARD_VECTOR_SIZE;
	return std::min(100.0, 100.0 * static_cast<double>(processed) / static_cast<double>(total));
}

unique_ptr<BaseStatistics> CityJSONStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                              column_t column_index) {
	// Return nullptr for MVP
	// Future: could return min/max for numeric columns
	return nullptr;
}

} // namespace cityjson
} // namespace duckdb

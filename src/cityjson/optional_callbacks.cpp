#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<NodeStatistics> CityJSONCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

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

	size_t total = bind_data.chunks.TotalCityObjectCount();
	if (total == 0)
		return 1.0;

	size_t processed = global_state.batch_index.load() * STANDARD_VECTOR_SIZE;
	return std::min(1.0, static_cast<double>(processed) / static_cast<double>(total));
}

unique_ptr<BaseStatistics> CityJSONStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                              column_t column_index) {
	// Return nullptr for MVP
	// Future: could return min/max for numeric columns
	return nullptr;
}

} // namespace cityjson
} // namespace duckdb

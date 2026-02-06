#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<LocalTableFunctionState> CityJSONInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                      GlobalTableFunctionState *global_state) {
	auto result = make_uniq<CityJSONLocalState>();
	result->column_ids = input.column_ids;
	result->projection_ids = input.projection_ids;
	return std::move(result);
}

} // namespace cityjson
} // namespace duckdb

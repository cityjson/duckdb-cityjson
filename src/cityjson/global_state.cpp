#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

CityJSONGlobalState::CityJSONGlobalState() : batch_index(0) {
}

idx_t CityJSONGlobalState::MaxThreads() const {
	// Streaming and filtered reads use a single sequential source.
	if (streaming_reader != nullptr || has_filters) {
		return 1;
	}
	// Allow multi-threading for materialized data
	return DConstants::INVALID_INDEX;
}

} // namespace cityjson
} // namespace duckdb

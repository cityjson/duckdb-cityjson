#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

CityJSONGlobalState::CityJSONGlobalState() : batch_index(0) {
}

idx_t CityJSONGlobalState::MaxThreads() const {
	// Streaming reads from a single sequential source; restrict to one thread.
	if (streaming_reader != nullptr) {
		return 1;
	}
	// Allow multi-threading for materialized data
	return DConstants::INVALID_INDEX;
}

} // namespace cityjson
} // namespace duckdb

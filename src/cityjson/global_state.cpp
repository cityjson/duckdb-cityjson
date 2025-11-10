#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

CityJSONGlobalState::CityJSONGlobalState() : batch_index(0) {}

idx_t CityJSONGlobalState::MaxThreads() const {
    // Allow multi-threading
    return DConstants::INVALID_INDEX;
}

} // namespace cityjson
} // namespace duckdb

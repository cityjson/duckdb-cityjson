#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(
    ClientContext &context,
    TableFunctionInitInput &input) {

    return make_uniq<CityJSONGlobalState>();
}

} // namespace cityjson
} // namespace duckdb

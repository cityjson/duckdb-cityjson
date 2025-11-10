#include "cityjson/table_function.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {
namespace cityjson {

TableFunction CreateReadCityJSONTableFunction() {
    TableFunction func("read_cityjson", {LogicalType::VARCHAR}, CityJSONScan, CityJSONBind);

    // Set description
    func.description = "Read CityJSON or CityJSONSeq files";

    // Named parameters
    func.named_parameters["sample_lines"] = LogicalType::BIGINT;

    // Set callbacks
    func.init_global = CityJSONInitGlobal;
    func.init_local = CityJSONInitLocal;
    func.get_batch_index = nullptr;  // Use default
    func.cardinality = CityJSONCardinality;
    func.get_progress = CityJSONProgress;
    func.statistics = CityJSONStatistics;

    // Enable projection pushdown
    func.projection_pushdown = true;

    // Filter pushdown not implemented yet
    // func.filter_pushdown = false;

    return func;
}

void RegisterCityJSONTableFunction(DatabaseInstance &db) {
    auto func = CreateReadCityJSONTableFunction();
    ExtensionUtil::RegisterFunction(db, func);
}

} // namespace cityjson
} // namespace duckdb

#include "cityjson/table_function.hpp"
#include "cityjson/reader.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<CityJSONGlobalState>();
	auto &bind_data = input.bind_data->Cast<CityJSONBindData>();

	if (bind_data.streaming) {
		try {
			auto reader = OpenAnyCityJSONFile(context, bind_data.file_name);
			result->chunks = reader->ReadAllChunks();
			result->scan_plan = result->chunks.BuildScanPlan();
		} catch (const CityJSONError &e) {
			throw InternalException("Failed to read streaming CityJSON data: " + std::string(e.what()));
		}
	}

	return result;
}

} // namespace cityjson
} // namespace duckdb

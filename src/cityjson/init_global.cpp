#include "cityjson/table_function.hpp"
#include "cityjson/reader.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<CityJSONGlobalState>();
	auto &bind_data = input.bind_data->Cast<CityJSONBindData>();

	if (bind_data.streaming) {
		try {
			result->streaming_reader = OpenAnyCityJSONFile(context, bind_data.file_name);
			// Consume metadata so the scan starts at the first feature line.
			result->streaming_reader->ReadMetadata();
		} catch (const CityJSONError &e) {
			throw InternalException("Failed to open streaming CityJSON reader: " + std::string(e.what()));
		}
	}

	return result;
}

} // namespace cityjson
} // namespace duckdb

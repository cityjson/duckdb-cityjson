#include "cityjson/table_function.hpp"
#include "cityjson/reader.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<CityJSONGlobalState>();
	auto &bind_data = input.bind_data->Cast<CityJSONBindData>();
	result->has_filters = !bind_data.equality_filters.empty();

	if (bind_data.streaming) {
		try {
			// Replay the bind's own choice of factory and sampling depth rather than
			// auto-detecting afresh: re-detection can pick a different reader for the same
			// path than the one the schema was bound against. See ReaderKind (reader.hpp).
			result->streaming_reader =
			    OpenCityJSONFileOfKind(context, bind_data.reader_kind, bind_data.file_name, bind_data.sample_lines);
			// Consume metadata so the scan starts at the first feature line.
			result->streaming_reader->ReadMetadata();
		} catch (const CityJSONError &e) {
			// Opening and parsing a user-supplied file is not an invariant of ours;
			// InternalException would render this as "please file a bug" and, being
			// session-fatal, would take the connection down with it.
			throw IOException("Failed to open streaming CityJSON reader: " + std::string(e.what()));
		}
	}

	return result;
}

} // namespace cityjson
} // namespace duckdb

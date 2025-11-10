#include "cityjson/table_function.hpp"
#include "cityjson/reader.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<FunctionData> CityJSONBind(
    ClientContext &context,
    TableFunctionBindInput &input,
    vector<LogicalType> &return_types,
    vector<string> &names) {

    auto result = make_uniq<CityJSONBindData>();

    // Get file_name from first positional parameter
    if (input.inputs.empty()) {
        throw BinderException("read_cityjson requires a file path");
    }
    result->file_name = StringValue::Get(input.inputs[0]);

    // Parse named parameters (sample_lines is optional, default handled by reader)
    // For now, we use the default sample_lines in the reader

    // Open reader
    unique_ptr<CityJSONReader> reader;
    try {
        reader = OpenAnyCityJSONFile(result->file_name);
    } catch (const CityJSONError& e) {
        throw BinderException("Failed to open CityJSON file: " + std::string(e.what()));
    }

    // Read metadata
    try {
        result->metadata = reader->ReadMetadata();
    } catch (const CityJSONError& e) {
        throw BinderException("Failed to read CityJSON metadata: " + std::string(e.what()));
    }

    // Infer schema from samples
    try {
        result->columns = reader->Columns();
    } catch (const CityJSONError& e) {
        throw BinderException("Failed to infer schema: " + std::string(e.what()));
    }

    // Load all data
    try {
        result->chunks = reader->ReadAllChunks();
    } catch (const CityJSONError& e) {
        throw BinderException("Failed to read CityJSON data: " + std::string(e.what()));
    }

    // Populate return types and names
    for (const auto& col : result->columns) {
        names.push_back(col.name);
        return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
    }

    return std::move(result);
}

} // namespace cityjson
} // namespace duckdb

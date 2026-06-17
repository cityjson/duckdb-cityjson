#include "cityjson/table_function.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/column_types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace cityjson {

CityJSONReadOptions ParseCityJSONReadOptions(const TableFunctionBindInput &input, const std::string &function_name) {
	CityJSONReadOptions options;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "lod") {
			options.target_lod = LODTableUtils::NormalizeLOD(StringValue::Get(kv.second));
			options.use_wkb_encoding = true; // Enable WKB encoding when LOD is specified
		} else if (kv.first == "sample_lines") {
			auto sample_lines = BigIntValue::Get(kv.second);
			if (sample_lines < 0) {
				throw BinderException(function_name + ": sample_lines must be non-negative");
			}
			options.sample_lines = static_cast<size_t>(sample_lines);
		}
	}

	return options;
}

static std::vector<CityJSONFeature> FlattenChunks(const CityJSONFeatureChunk &chunks) {
	std::vector<CityJSONFeature> all_features;
	for (size_t i = 0; i < chunks.ChunkCount(); i++) {
		auto chunk = chunks.GetChunk(i);
		if (chunk) {
			all_features.insert(all_features.end(), chunk->begin(), chunk->end());
		}
	}
	return all_features;
}

static void InferSchema(CityJSONBindData &bind_data, CityJSONReader &reader) {
	if (bind_data.target_lod.has_value()) {
		auto all_features = FlattenChunks(bind_data.chunks);
		auto lod_tables = LODTableUtils::InferLODTables(all_features);

		bool found = false;
		for (const auto &table : lod_tables) {
			if (table.lod_value == bind_data.target_lod.value()) {
				bind_data.columns = table.columns;
				found = true;
				break;
			}
		}

		if (!found) {
			throw BinderException("LOD '" + bind_data.target_lod.value() + "' not found in file. Available LODs: " +
			                      (lod_tables.empty() ? "none" : lod_tables[0].lod_value));
		}
	} else {
		try {
			bind_data.columns = reader.Columns();
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to infer schema: " + std::string(e.what()));
		}
	}
}

unique_ptr<FunctionData> BindCityJSONRead(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names,
                                          const std::string &function_name,
                                          std::unique_ptr<CityJSONReader> reader, bool streaming) {
	auto result = make_uniq<CityJSONBindData>();

	if (input.inputs.empty()) {
		throw BinderException(function_name + " requires a file path");
	}
	result->file_name = StringValue::Get(input.inputs[0]);
	result->streaming = streaming;

	auto options = ParseCityJSONReadOptions(input, function_name);
	result->target_lod = options.target_lod;
	result->use_wkb_encoding = options.use_wkb_encoding;

	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read metadata: " + std::string(e.what()));
	}

	if (!streaming) {
		try {
			result->chunks = reader->ReadAllChunks();
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to read data: " + std::string(e.what()));
		}
		result->scan_plan = result->chunks.BuildScanPlan();
	}

	InferSchema(*result, *reader);

	for (const auto &col : result->columns) {
		names.push_back(col.name);
		return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
	}

	return result;
}

unique_ptr<FunctionData> CityJSONBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("read_cityjson requires a file path");
	}
	std::string file_name = StringValue::Get(input.inputs[0]);
	auto options = ParseCityJSONReadOptions(input, "read_cityjson");

	std::unique_ptr<CityJSONReader> reader;
	try {
		reader = OpenAnyCityJSONFile(context, file_name, options.sample_lines);
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to open CityJSON file: " + std::string(e.what()));
	}

	return BindCityJSONRead(context, input, return_types, names, "read_cityjson", std::move(reader));
}

unique_ptr<FunctionData> CityJSONSeqBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("read_cityjsonseq requires a file path");
	}
	std::string file_name = StringValue::Get(input.inputs[0]);
	auto options = ParseCityJSONReadOptions(input, "read_cityjsonseq");

	std::unique_ptr<CityJSONReader> reader;
	try {
		std::string content = json_utils::ReadFileContent(context, file_name);
		reader = std::make_unique<LocalCityJSONSeqReader>(file_name, std::move(content), options.sample_lines);
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to open CityJSONSeq file: " + std::string(e.what()));
	}

	return BindCityJSONRead(context, input, return_types, names, "read_cityjsonseq", std::move(reader), true);
}

} // namespace cityjson
} // namespace duckdb

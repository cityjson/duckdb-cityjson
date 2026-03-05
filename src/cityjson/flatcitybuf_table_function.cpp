#ifdef CITYJSON_HAS_FCB

#include "cityjson/flatcitybuf_table_function.hpp"
#include "cityjson/flatcitybuf_reader.hpp"
#include "cityjson/table_function.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/metadata_table.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/column_types.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

// ============================================================
// read_flatcitybuf Bind
// ============================================================

static unique_ptr<FunctionData> FlatCityBufBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CityJSONBindData>();

	if (input.inputs.empty()) {
		throw BinderException("read_flatcitybuf requires a file path");
	}
	result->file_name = StringValue::Get(input.inputs[0]);

	for (auto &kv : input.named_parameters) {
		if (kv.first == "lod") {
			result->target_lod = StringValue::Get(kv.second);
			result->use_wkb_encoding = true;
		}
	}

	// Read FCB file via DuckDB FileSystem
	std::string content = json_utils::ReadFileContent(context, result->file_name);
	auto reader = std::make_unique<FlatCityBufReader>(result->file_name, std::move(content));

	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read FlatCityBuf metadata: " + std::string(e.what()));
	}

	try {
		result->chunks = reader->ReadAllChunks();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read FlatCityBuf data: " + std::string(e.what()));
	}

	if (result->target_lod.has_value()) {
		std::vector<CityJSONFeature> all_features;
		for (size_t i = 0; i < result->chunks.ChunkCount(); i++) {
			auto chunk = result->chunks.GetChunk(i);
			if (chunk) {
				all_features.insert(all_features.end(), chunk->begin(), chunk->end());
			}
		}

		auto lod_tables = LODTableUtils::InferLODTables(all_features);
		bool found = false;
		for (const auto &table : lod_tables) {
			if (table.lod_value == result->target_lod.value()) {
				result->columns = table.columns;
				found = true;
				break;
			}
		}

		if (!found) {
			throw BinderException("LOD '" + result->target_lod.value() + "' not found in FlatCityBuf file");
		}
	} else {
		try {
			result->columns = reader->Columns();
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to infer schema: " + std::string(e.what()));
		}
	}

	for (const auto &col : result->columns) {
		names.push_back(col.name);
		return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
	}

	return std::move(result);
}

// ============================================================
// read_flatcitybuf Registration
// ============================================================

void RegisterFlatCityBufTableFunction(ExtensionLoader &loader) {
	TableFunction func("read_flatcitybuf", {LogicalType::VARCHAR}, CityJSONScan, FlatCityBufBind);

	func.named_parameters["sample_lines"] = LogicalType::BIGINT;
	func.named_parameters["lod"] = LogicalType::VARCHAR;

	func.init_global = CityJSONInitGlobal;
	func.init_local = CityJSONInitLocal;
	func.cardinality = CityJSONCardinality;
	func.statistics = CityJSONStatistics;
	func.projection_pushdown = true;

	loader.RegisterFunction(func);
}

// ============================================================
// flatcitybuf_metadata
// ============================================================

struct FcbMetadataBindData : public TableFunctionData {
	std::string file_name;
	CityJSON metadata;
	idx_t city_objects_count;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<FcbMetadataBindData>();
		result->file_name = file_name;
		result->metadata = metadata;
		result->city_objects_count = city_objects_count;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		auto &other_data = other.Cast<FcbMetadataBindData>();
		return file_name == other_data.file_name;
	}
};

struct FcbMetadataGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t MaxThreads() const override { return 1; }
};

static unique_ptr<FunctionData> FcbMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FcbMetadataBindData>();
	result->file_name = StringValue::Get(input.inputs[0]);

	std::string content = json_utils::ReadFileContent(context, result->file_name);
	auto reader = std::make_unique<FlatCityBufReader>(result->file_name, std::move(content));

	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read FlatCityBuf metadata: " + std::string(e.what()));
	}

	result->city_objects_count = 0;
	try {
		auto chunks = reader->ReadAllChunks();
		for (size_t i = 0; i < chunks.ChunkCount(); i++) {
			auto chunk = chunks.GetChunk(i);
			if (chunk) {
				for (const auto &feature : *chunk) {
					result->city_objects_count += feature.city_objects.size();
				}
			}
		}
	} catch (const CityJSONError &) {
		result->city_objects_count = 0;
	}

	return_types = MetadataTableUtils::GetMetadataTableTypes();
	names = MetadataTableUtils::GetMetadataTableNames();

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> FcbMetadataInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	return make_uniq<FcbMetadataGlobalState>();
}

static void FcbMetadataScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<FcbMetadataBindData>();
	auto &global_state = data.global_state->Cast<FcbMetadataGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	auto metadata_chunk = MetadataTableUtils::CreateMetadataChunk(bind_data.metadata, bind_data.city_objects_count);
	output.SetCardinality(1);
	for (idx_t col = 0; col < metadata_chunk->ColumnCount(); col++) {
		output.data[col].Reference(metadata_chunk->data[col]);
	}

	global_state.done = true;
}

void RegisterFlatCityBufMetadataTableFunction(ExtensionLoader &loader) {
	TableFunction func("flatcitybuf_metadata", {LogicalType::VARCHAR}, FcbMetadataScan, FcbMetadataBind);
	func.init_global = FcbMetadataInitGlobal;
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB

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
#include "fcb.h"

namespace duckdb {
namespace cityjson {

// ============================================================
// read_flatcitybuf Bind
// ============================================================

static unique_ptr<FunctionData> FlatCityBufBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("read_flatcitybuf requires a file path");
	}
	std::string file_name = StringValue::Get(input.inputs[0]);
	auto options = ParseCityJSONReadOptions(input, "read_flatcitybuf");

	auto reader = std::make_unique<FlatCityBufReader>(file_name, file_name, options.sample_lines);

	return BindCityJSONRead(context, input, return_types, names, "read_flatcitybuf", std::move(reader));
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
		return result;
	}

	bool Equals(const FunctionData &other) const override {
		auto &other_data = other.Cast<FcbMetadataBindData>();
		return file_name == other_data.file_name;
	}
};

struct FcbMetadataGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> FcbMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<FcbMetadataBindData>();
	result->file_name = StringValue::Get(input.inputs[0]);

	auto reader = std::make_unique<FlatCityBufReader>(result->file_name, result->file_name);

	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read FlatCityBuf metadata: " + std::string(e.what()));
	}

	// Use features_count from FCB metadata for a fast count without reading all features
	auto fcb_reader_raw = fcb::fcb_reader_open(result->file_name);
	auto fcb_meta = fcb::fcb_reader_metadata(*fcb_reader_raw);
	result->city_objects_count = fcb_meta.features_count;

	return_types = MetadataTableUtils::GetMetadataTableTypes();
	names = MetadataTableUtils::GetMetadataTableNames();

	return result;
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

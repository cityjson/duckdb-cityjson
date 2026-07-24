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
// FlatCityBufBindData
// ============================================================

unique_ptr<FunctionData> FlatCityBufBindData::Copy() const {
	auto result = make_uniq<FlatCityBufBindData>();
	result->file_name = file_name;
	result->metadata = metadata;
	result->chunks = chunks;
	result->scan_plan = scan_plan;
	result->columns = columns;
	result->target_lod = target_lod;
	result->use_wkb_encoding = use_wkb_encoding;
	result->streaming = streaming;
	result->equality_filters = equality_filters;
	result->bbox = bbox;
	result->reader = reader;
	return result;
}

bool FlatCityBufBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<FlatCityBufBindData>();
	return file_name == other.file_name && target_lod == other.target_lod &&
	       use_wkb_encoding == other.use_wkb_encoding && streaming == other.streaming &&
	       equality_filters == other.equality_filters && bbox == other.bbox;
}

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

	auto reader = std::make_shared<FlatCityBufReader>(context, file_name, file_name, options.sample_lines);

	bool has_min_x = input.named_parameters.count("min_x") > 0;
	bool has_min_y = input.named_parameters.count("min_y") > 0;
	bool has_max_x = input.named_parameters.count("max_x") > 0;
	bool has_max_y = input.named_parameters.count("max_y") > 0;
	std::optional<std::array<double, 4>> bbox;
	if (has_min_x || has_min_y || has_max_x || has_max_y) {
		if (!(has_min_x && has_min_y && has_max_x && has_max_y)) {
			throw BinderException("read_flatcitybuf: min_x, min_y, max_x, and max_y must all be given together");
		}
		bbox = std::array<double, 4> {DoubleValue::Get(input.named_parameters.at("min_x")),
		                              DoubleValue::Get(input.named_parameters.at("min_y")),
		                              DoubleValue::Get(input.named_parameters.at("max_x")),
		                              DoubleValue::Get(input.named_parameters.at("max_y"))};
		reader->SetBBoxFilter(bbox.value());
	}

	auto generic = BindCityJSONReadRaw(context, input, return_types, names, "read_flatcitybuf", *reader, false);

	// Field-by-field, matching CityJSONBindData::Copy()'s own pattern (bind_data.cpp) --
	// deliberately not a whole-object copy-assignment through a CityJSONBindData&
	// reference, to sidestep any question about FunctionData's (deprecated-but-legal)
	// implicitly-generated copy assignment operator.
	auto result = make_uniq<FlatCityBufBindData>();
	result->file_name = generic.file_name;
	result->metadata = generic.metadata;
	result->chunks = generic.chunks;
	result->scan_plan = generic.scan_plan;
	result->columns = generic.columns;
	result->target_lod = generic.target_lod;
	result->use_wkb_encoding = generic.use_wkb_encoding;
	result->streaming = false;
	result->bbox = bbox;
	result->reader = reader;
	return result;
}

// ============================================================
// read_flatcitybuf Registration
// ============================================================

void RegisterFlatCityBufTableFunction(ExtensionLoader &loader) {
	TableFunction func("read_flatcitybuf", {LogicalType::VARCHAR}, CityJSONScan, FlatCityBufBind);

	func.named_parameters["sample_lines"] = LogicalType::BIGINT;
	func.named_parameters["lod"] = LogicalType::VARCHAR;
	func.named_parameters["min_x"] = LogicalType::DOUBLE;
	func.named_parameters["min_y"] = LogicalType::DOUBLE;
	func.named_parameters["max_x"] = LogicalType::DOUBLE;
	func.named_parameters["max_y"] = LogicalType::DOUBLE;

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

	auto reader = std::make_unique<FlatCityBufReader>(context, result->file_name, result->file_name);

	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read FlatCityBuf metadata: " + std::string(e.what()));
	}

	// features_count comes straight from the header -- no separate reopen needed now
	// that FlatCityBufReader exposes Header() directly.
	result->city_objects_count = reader->Header().info().features_count;

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

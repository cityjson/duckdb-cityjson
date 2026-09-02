#include "cityjson/obj_table_function.hpp"

#include "cityjson/appearance_table_function.hpp"
#include "cityjson/crs_projjson.hpp"
#include "cityjson/error.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/metadata_table.hpp"
#include "cityjson/table_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {
namespace cityjson {

OBJReadOptions ParseOBJReadOptions(const TableFunctionBindInput &input, const std::string &function_name) {
	OBJReadOptions options;
	bool has_lod = false;
	for (auto &kv : input.named_parameters) {
		if (kv.first == "lod") {
			options.lod = LODTableUtils::NormalizeLOD(StringValue::Get(kv.second));
			has_lod = true;
		} else if (kv.first == "object_type") {
			options.object_type = StringValue::Get(kv.second);
			if (options.object_type.empty()) {
				throw BinderException(function_name + ": object_type must not be empty");
			}
		} else if (kv.first == "geometry_type") {
			options.geometry_type = StringValue::Get(kv.second);
			if (options.geometry_type != "auto" && options.geometry_type != "Solid" &&
			    options.geometry_type != "MultiSurface") {
				throw BinderException(function_name +
				                      ": geometry_type must be 'auto', 'Solid' or 'MultiSurface', got '" +
				                      options.geometry_type + "'");
			}
		}
	}
	if (!has_lod) {
		throw BinderException(function_name + ": lod is required -- an OBJ carries no level of detail, and the value "
		                                      "names the geometry columns (e.g. lod := '2.2')");
	}
	return options;
}

static unique_ptr<FunctionData> OBJBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("read_obj requires a file path");
	}
	std::string file_name = StringValue::Get(input.inputs[0]);
	auto options = ParseOBJReadOptions(input, "read_obj");
	std::unique_ptr<CityJSONReader> reader = std::make_unique<OBJReader>(context, file_name, options);
	// Non-streaming: the whole file is parsed at bind and the chunks live in the bind
	// data, so init_global never has to reopen a reader (ReaderKind is irrelevant here).
	return BindCityJSONRead(context, input, return_types, names, "read_obj", std::move(reader));
}

namespace {

struct OBJMetadataBindData : public TableFunctionData {
	std::string file_name;
	CityJSON metadata;
	idx_t count = 0;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<OBJMetadataBindData>();
		result->file_name = file_name;
		result->metadata = metadata;
		result->count = count;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<OBJMetadataBindData>();
		return file_name == o.file_name && metadata.metadata.has_value() == o.metadata.metadata.has_value() &&
		       (!metadata.metadata.has_value() ||
		        metadata.metadata->reference_system == o.metadata.metadata->reference_system);
	}
};

struct OBJMetadataGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<FunctionData> OBJMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.empty()) {
		throw BinderException("obj_metadata requires a file path");
	}
	auto result = make_uniq<OBJMetadataBindData>();
	result->file_name = StringValue::Get(input.inputs[0]);

	OBJReadOptions options;
	options.lod = "0.0"; // irrelevant to metadata; the reader needs one
	for (auto &kv : input.named_parameters) {
		if (kv.first == "crs") {
			auto text = StringValue::Get(kv.second);
			auto code = EpsgCodeFromReferenceSystem(text);
			if (!code.has_value()) {
				throw BinderException("obj_metadata: crs '" + text +
				                      "' is not an EPSG reference (use EPSG:7415, "
				                      "urn:ogc:def:crs:EPSG::7415 or https://www.opengis.net/def/crs/EPSG/0/7415)");
			}
			options.crs = "https://www.opengis.net/def/crs/EPSG/0/" + std::to_string(code.value());
		}
	}

	OBJReader reader(context, result->file_name, options);
	try {
		result->metadata = reader.ReadMetadata();
		result->count = reader.CountCityObjects();
	} catch (const CityJSONError &e) {
		throw BinderException("obj_metadata: failed to read '" + result->file_name + "': " + e.what());
	}
	return_types = MetadataTableUtils::GetMetadataTableTypes();
	names = MetadataTableUtils::GetMetadataTableNames();
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> OBJMetadataInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<OBJMetadataGlobalState>();
}

void OBJMetadataScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<OBJMetadataBindData>();
	auto &state = data.global_state->Cast<OBJMetadataGlobalState>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}
	auto chunk = MetadataTableUtils::CreateMetadataChunk(bind_data.metadata, optional_idx(bind_data.count),
	                                                     optional_idx(bind_data.count));
	output.SetCardinality(1);
	for (idx_t col = 0; col < chunk->ColumnCount(); col++) {
		output.data[col].Reference(chunk->data[col]);
	}
	state.done = true;
}

} // namespace

void RegisterOBJTableFunctions(ExtensionLoader &loader) {
	TableFunction read_obj("read_obj", {LogicalType::VARCHAR}, CityJSONScan, OBJBind);
	read_obj.named_parameters["lod"] = LogicalType::VARCHAR;
	read_obj.named_parameters["object_type"] = LogicalType::VARCHAR;
	read_obj.named_parameters["geometry_type"] = LogicalType::VARCHAR;
	read_obj.named_parameters["appearance"] = LogicalType::VARCHAR; // 'local' (default) or 'sidecar'
	read_obj.named_parameters["sample_lines"] = LogicalType::BIGINT;
	read_obj.init_global = CityJSONInitGlobal;
	read_obj.init_local = CityJSONInitLocal;
	read_obj.cardinality = CityJSONCardinality;
	read_obj.statistics = CityJSONStatistics;
	read_obj.table_scan_progress = CityJSONProgress;
	read_obj.projection_pushdown = true;
	read_obj.filter_pushdown = false;
	read_obj.pushdown_complex_filter = CityJSONPushdownComplexFilter;
	loader.RegisterFunction(read_obj);

	// Sidecars need no LoD: the materials are file-global. Any lod satisfies the reader.
	ReaderOpener obj_opener = [](ClientContext &context, const std::string &path) {
		OBJReadOptions options;
		options.lod = "0.0";
		return std::unique_ptr<CityJSONReader>(std::make_unique<OBJReader>(context, path, options));
	};
	loader.RegisterFunction(CreateAppearanceTableFunction("obj_materials", SidecarKind::MATERIALS, obj_opener));
	loader.RegisterFunction(CreateAppearanceTableFunction("obj_textures", SidecarKind::TEXTURES, obj_opener));

	TableFunction obj_metadata("obj_metadata", {LogicalType::VARCHAR}, OBJMetadataScan, OBJMetadataBind);
	obj_metadata.named_parameters["crs"] = LogicalType::VARCHAR;
	obj_metadata.init_global = OBJMetadataInitGlobal;
	loader.RegisterFunction(obj_metadata);
}

} // namespace cityjson
} // namespace duckdb

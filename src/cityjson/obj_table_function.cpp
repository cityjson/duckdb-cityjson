#include "cityjson/obj_table_function.hpp"

#include "cityjson/error.hpp"
#include "cityjson/lod_table.hpp"
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
}

} // namespace cityjson
} // namespace duckdb

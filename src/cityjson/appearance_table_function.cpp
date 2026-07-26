#include "cityjson/appearance_table_function.hpp"

#include "cityjson/error.hpp"
#include "cityjson/appearance_normalise.hpp"
#include "cityjson/reader.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace cityjson {

namespace {

enum class SidecarKind { MATERIALS, TEXTURES };

struct AppearanceBindData : public TableFunctionData {
	std::string file_name;
	SidecarKind kind;
	AppearanceIndex index;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<AppearanceBindData>();
		result->file_name = file_name;
		result->kind = kind;
		result->index = index;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<AppearanceBindData>();
		return file_name == o.file_name && kind == o.kind;
	}
};

struct AppearanceGlobalState : public GlobalTableFunctionState {
	idx_t offset = 0;
	idx_t MaxThreads() const override {
		return 1;
	}
};

Value DoubleListOrNull(const std::optional<std::vector<double>> &values) {
	if (!values.has_value()) {
		return Value(LogicalType::LIST(LogicalType(LogicalTypeId::DOUBLE)));
	}
	duckdb::vector<Value> children;
	for (const auto value : values.value()) {
		children.push_back(Value::DOUBLE(value));
	}
	return Value::LIST(LogicalType(LogicalTypeId::DOUBLE), std::move(children));
}

Value StringOrNull(const std::optional<std::string> &value) {
	return value.has_value() ? Value(value.value()) : Value(LogicalType(LogicalTypeId::VARCHAR));
}

Value DoubleOrNull(const std::optional<double> &value) {
	return value.has_value() ? Value::DOUBLE(value.value()) : Value(LogicalType(LogicalTypeId::DOUBLE));
}

Value OtherOrNull(const json &other) {
	if (other.is_null()) {
		return Value(LogicalType(LogicalTypeId::VARCHAR));
	}
	return Value(other.dump());
}

unique_ptr<FunctionData> AppearanceBind(ClientContext &context, TableFunctionBindInput &input,
                                        vector<LogicalType> &return_types, vector<string> &names,
                                        SidecarKind kind, const char *function_name) {
	auto result = make_uniq<AppearanceBindData>();
	result->file_name = StringValue::Get(input.inputs[0]);
	result->kind = kind;

	std::unique_ptr<CityJSONReader> reader;
	try {
		reader = OpenAnyCityJSONFile(context, result->file_name);
		auto metadata = reader->ReadMetadata();
		// The whole file, not a sample: an appearance definition used by one feature in
		// the tail is as much a part of the dataset's sidecar as one in the header, and
		// omitting it would leave that feature's references dangling.
		auto all = reader->ReadAllChunks();
		result->index = AppearanceIndex::Build(metadata, all.records);
	} catch (const CityJSONError &e) {
		throw BinderException("%s: failed to read '%s': %s", function_name, result->file_name, e.what());
	}

	const auto varchar = LogicalType(LogicalTypeId::VARCHAR);
	const auto dbl = LogicalType(LogicalTypeId::DOUBLE);
	const auto dbl_list = LogicalType::LIST(LogicalType(LogicalTypeId::DOUBLE));

	if (kind == SidecarKind::MATERIALS) {
		names = {"id",           "name",        "ambientIntensity", "diffuseColor", "specularColor",
		         "emissiveColor", "transparency", "shininess",       "isSmooth",     "other"};
		return_types = {LogicalType(LogicalTypeId::BIGINT),
		                varchar,
		                dbl,
		                dbl_list,
		                dbl_list,
		                dbl_list,
		                dbl,
		                dbl,
		                LogicalType(LogicalTypeId::BOOLEAN),
		                varchar};
	} else {
		names = {"id", "image_uri", "image_data", "image_type", "wrapMode", "textureType", "borderColor", "other"};
		return_types = {LogicalType(LogicalTypeId::BIGINT),
		                varchar,
		                LogicalType(LogicalTypeId::BLOB),
		                varchar,
		                varchar,
		                varchar,
		                dbl_list,
		                varchar};
	}
	return std::move(result);
}

unique_ptr<FunctionData> MaterialsBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	return AppearanceBind(context, input, return_types, names, SidecarKind::MATERIALS, "cityjson_materials");
}

unique_ptr<FunctionData> TexturesBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	return AppearanceBind(context, input, return_types, names, SidecarKind::TEXTURES, "cityjson_textures");
}

unique_ptr<GlobalTableFunctionState> AppearanceInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<AppearanceGlobalState>();
}

void AppearanceScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<AppearanceBindData>();
	auto &state = data.global_state->Cast<AppearanceGlobalState>();

	const auto total = bind_data.kind == SidecarKind::MATERIALS ? bind_data.index.materials.size()
	                                                            : bind_data.index.textures.size();
	idx_t emitted = 0;
	while (state.offset < total && emitted < STANDARD_VECTOR_SIZE) {
		const auto index = state.offset;
		// id is the ordinal position in the source array -- the value a geometry's
		// appearance map references.
		output.SetValue(0, emitted, Value::BIGINT(static_cast<int64_t>(index)));

		if (bind_data.kind == SidecarKind::MATERIALS) {
			const auto &material = bind_data.index.materials[index];
			output.SetValue(1, emitted, StringOrNull(material.name));
			output.SetValue(2, emitted, DoubleOrNull(material.ambient_intensity));
			output.SetValue(3, emitted, DoubleListOrNull(material.diffuse_color));
			output.SetValue(4, emitted, DoubleListOrNull(material.specular_color));
			output.SetValue(5, emitted, DoubleListOrNull(material.emissive_color));
			output.SetValue(6, emitted, DoubleOrNull(material.transparency));
			output.SetValue(7, emitted, DoubleOrNull(material.shininess));
			output.SetValue(8, emitted,
			                material.is_smooth.has_value() ? Value::BOOLEAN(material.is_smooth.value())
			                                               : Value(LogicalType(LogicalTypeId::BOOLEAN)));
			output.SetValue(9, emitted, OtherOrNull(material.other));
		} else {
			const auto &texture = bind_data.index.textures[index];
			output.SetValue(1, emitted, StringOrNull(texture.image_uri));
			// image_data has no CityJSON source: the format references images by URI.
			output.SetValue(2, emitted, Value(LogicalType(LogicalTypeId::BLOB)));
			output.SetValue(3, emitted, StringOrNull(texture.image_type));
			output.SetValue(4, emitted, StringOrNull(texture.wrap_mode));
			output.SetValue(5, emitted, StringOrNull(texture.texture_type));
			output.SetValue(6, emitted, DoubleListOrNull(texture.border_color));
			output.SetValue(7, emitted, OtherOrNull(texture.other));
		}

		state.offset++;
		emitted++;
	}
	output.SetCardinality(emitted);
}

} // namespace

void RegisterAppearanceTableFunctions(ExtensionLoader &loader) {
	TableFunction materials("cityjson_materials", {LogicalType(LogicalTypeId::VARCHAR)}, AppearanceScan, MaterialsBind);
	materials.init_global = AppearanceInitGlobal;
	loader.RegisterFunction(materials);

	TableFunction textures("cityjson_textures", {LogicalType(LogicalTypeId::VARCHAR)}, AppearanceScan, TexturesBind);
	textures.init_global = AppearanceInitGlobal;
	loader.RegisterFunction(textures);
}

} // namespace cityjson
} // namespace duckdb

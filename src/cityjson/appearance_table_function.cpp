#include "cityjson/appearance_table_function.hpp"

#include "cityjson/error.hpp"
#include "cityjson/appearance_normalise.hpp"
#include "cityjson/city_object_utils.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/reader.hpp"
#include "duckdb/common/exception.hpp"

#include <set>

namespace duckdb {
namespace cityjson {

namespace {

enum class SidecarKind { MATERIALS, TEXTURES, TEMPLATES };

struct AppearanceBindData : public TableFunctionData {
	std::string file_name;
	SidecarKind kind;
	AppearanceIndex index;
	GeometryTemplates templates;
	// The distinct LoDs across all templates, in sorted order. Each contributes four
	// columns, exactly as an object table does, so a template's LoD is carried by its
	// column name rather than by a value.
	std::vector<std::string> template_lods;
	// The document appearance's UV pool: what a template's texture rings index into.
	std::vector<std::array<double, 2>> template_uv_pool;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<AppearanceBindData>();
		result->file_name = file_name;
		result->kind = kind;
		result->index = index;
		result->templates = templates;
		result->template_lods = template_lods;
		result->template_uv_pool = template_uv_pool;
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
                                        vector<LogicalType> &return_types, vector<string> &names, SidecarKind kind,
                                        const char *function_name) {
	auto result = make_uniq<AppearanceBindData>();
	result->file_name = StringValue::Get(input.inputs[0]);
	result->kind = kind;

	std::unique_ptr<CityJSONReader> reader;
	try {
		reader = OpenAnyCityJSONFile(context, result->file_name);
		auto metadata = reader->ReadMetadata();
		if (kind == SidecarKind::TEMPLATES) {
			// Geometry templates are document-level and live entirely in the header, and
			// a template's appearance can only reference the header's definitions. So
			// build the index from the header alone and skip the feature scan -- reading
			// every chunk merely to count templates would parse a multi-gigabyte sequence
			// end to end.
			result->index = AppearanceIndex::Build(metadata, {});
			if (metadata.geometry_templates.has_value()) {
				result->templates = metadata.geometry_templates.value();
			}
			if (metadata.appearance.has_value()) {
				result->template_uv_pool = metadata.appearance->vertices_texture;
			}
		} else {
			// Reading the whole file, not a sample: a definition used only by a feature in
			// the tail belongs in the dataset's sidecar just as much as a header one, and
			// omitting it would leave that feature's references dangling.
			auto all = reader->ReadAllChunks();
			result->index = AppearanceIndex::Build(metadata, all.records);
		}
	} catch (const CityJSONError &e) {
		throw BinderException("%s: failed to read '%s': %s", function_name, result->file_name, e.what());
	}

	std::vector<std::string> sidecar_names;
	std::vector<LogicalType> sidecar_types;
	if (kind == SidecarKind::TEMPLATES) {
		GeometryTemplateColumns(result->templates, sidecar_names, sidecar_types, result->template_lods);
	} else {
		AppearanceSidecarColumns(kind == SidecarKind::MATERIALS ? "materials" : "textures", sidecar_names,
		                         sidecar_types);
	}
	for (idx_t i = 0; i < sidecar_names.size(); i++) {
		names.push_back(sidecar_names[i]);
		return_types.push_back(sidecar_types[i]);
	}
	return std::move(result);
}

unique_ptr<FunctionData> MaterialsBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	return AppearanceBind(context, input, return_types, names, SidecarKind::MATERIALS, "cityjson_materials");
}

unique_ptr<FunctionData> TemplatesBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
	return AppearanceBind(context, input, return_types, names, SidecarKind::TEMPLATES, "cityjson_geometry_templates");
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

	idx_t total;
	switch (bind_data.kind) {
	case SidecarKind::MATERIALS:
		total = bind_data.index.materials.size();
		break;
	case SidecarKind::TEXTURES:
		total = bind_data.index.textures.size();
		break;
	default:
		total = bind_data.templates.templates.size();
		break;
	}
	idx_t emitted = 0;
	while (state.offset < total && emitted < STANDARD_VECTOR_SIZE) {
		const auto index = state.offset;
		// id is the ordinal position in the source array -- the value a geometry's
		// appearance map references.
		output.SetValue(0, emitted, Value::BIGINT(static_cast<int64_t>(index)));

		if (bind_data.kind == SidecarKind::TEMPLATES) {
			const auto &geometry = bind_data.templates.templates[index];
			const auto lod = LODTableUtils::NormalizeLOD(geometry.lod);
			output.SetValue(1, emitted, Value(LogicalType(LogicalTypeId::VARCHAR))); // name: none in CityJSON

			for (size_t l = 0; l < bind_data.template_lods.size(); l++) {
				const idx_t base = 2 + l * 4;
				const bool mine = bind_data.template_lods[l] == lod;
				if (!mine) {
					// A template populates only its own LoD's columns; the table is
					// sparse by construction, which is the cost of keeping one LoD-naming
					// rule across the whole format.
					for (idx_t c = 0; c < 4; c++) {
						output.SetValue(base + c, emitted, Value(output.data[base + c].GetType()));
					}
					continue;
				}
				// Template vertices are raw doubles in the template's own local frame, so
				// no dataset transform is applied -- templates are exempt from the file CRS.
				auto wkb = CityObjectUtils::GetGeometryWKB(geometry, bind_data.templates.vertices, std::nullopt);
				output.SetValue(base, emitted, Value::BLOB(wkb.data(), wkb.size()));
				auto props = CityObjectUtils::GetGeometryPropertiesStruct(geometry);
				output.SetValue(base + 1, emitted, Value(props.is_null() ? std::string() : props.dump()));
				// A template's appearance needs the same normalisation an object row's
				// does: emitted verbatim, its rings would keep source-local texture ids
				// and bare UV indices, which no consumer of the package can resolve.
				// Templates are document-level, so they carry no feature id and resolve
				// against the header's definitions and UV pool.
				static const std::vector<std::array<double, 2>> no_uvs;
				const auto &uv_pool = bind_data.template_uv_pool;
				output.SetValue(base + 2, emitted,
				                geometry.material.has_value()
				                    ? Value(NormaliseMaterialMap(geometry.material.value(), bind_data.index, "").dump())
				                    : Value(LogicalType(LogicalTypeId::VARCHAR)));
				output.SetValue(base + 3, emitted,
				                geometry.texture.has_value()
				                    ? Value(NormaliseTextureMap(geometry.texture.value(), bind_data.index, "",
				                                                uv_pool.empty() ? no_uvs : uv_pool)
				                                .dump())
				                    : Value(LogicalType(LogicalTypeId::VARCHAR)));
			}
		} else if (bind_data.kind == SidecarKind::MATERIALS) {
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

void AppearanceSidecarColumns(const std::string &sidecar, std::vector<std::string> &names,
                              std::vector<LogicalType> &types) {
	const auto varchar = LogicalType(LogicalTypeId::VARCHAR);
	const auto dbl = LogicalType(LogicalTypeId::DOUBLE);
	const auto dbl_list = LogicalType::LIST(LogicalType(LogicalTypeId::DOUBLE));

	// The specification's sidecar tables exactly, including its mixed casing
	// (`ambientIntensity`, `wrapMode`, `borderColor`) -- those are the spec's names, not a
	// style choice, and a package is read by matching them.
	if (sidecar == "materials") {
		names = {"id",           "name",      "ambientIntensity", "diffuseColor", "specularColor", "emissiveColor",
		         "transparency", "shininess", "isSmooth",         "other"};
		types = {LogicalType(LogicalTypeId::BIGINT),  varchar, dbl, dbl_list, dbl_list, dbl_list, dbl, dbl,
		         LogicalType(LogicalTypeId::BOOLEAN), varchar};
		return;
	}
	names = {"id", "image_uri", "image_data", "image_type", "wrapMode", "textureType", "borderColor", "other"};
	types = {LogicalType(LogicalTypeId::BIGINT),
	         varchar,
	         LogicalType(LogicalTypeId::BLOB),
	         varchar,
	         varchar,
	         varchar,
	         dbl_list,
	         varchar};
}

void GeometryTemplateColumns(const GeometryTemplates &templates, std::vector<std::string> &names,
                             std::vector<LogicalType> &types, std::vector<std::string> &lods) {
	std::set<std::string> distinct_lods;
	for (idx_t i = 0; i < templates.templates.size(); i++) {
		const auto &geometry = templates.templates[i];
		// A template's LoD becomes part of its column names, so it must satisfy the LoD
		// suffix grammar. An absent or non-numeric lod would yield `geometry_lod` or
		// `geometry_lodfoo`, which no conforming reader -- including this extension's own
		// appearance-column discovery -- will recognise.
		if (geometry.lod.empty()) {
			throw BinderException("cityjson_geometry_templates: template %llu has no lod; a template's LoD names "
			                      "its columns, so it cannot be omitted",
			                      static_cast<uint64_t>(i));
		}
		try {
			std::stod(geometry.lod);
		} catch (const std::exception &) {
			throw BinderException("cityjson_geometry_templates: template %llu has a non-numeric lod '%s'; a "
			                      "template's LoD names its columns and must follow the LoD suffix grammar",
			                      static_cast<uint64_t>(i), geometry.lod);
		}
		distinct_lods.insert(LODTableUtils::NormalizeLOD(geometry.lod));
	}
	lods.assign(distinct_lods.begin(), distinct_lods.end());

	const auto varchar = LogicalType(LogicalTypeId::VARCHAR);

	// id is BIGINT so templates remap like the other sidecars when packages are merged;
	// `name` holds the source identifier, which CityJSON templates do not have (they are
	// array entries) but other sources may.
	names = {"id", "name"};
	types = {LogicalType(LogicalTypeId::BIGINT), varchar};
	for (const auto &lod : lods) {
		const auto suffix = LODTableUtils::FormatLODAsColumnSuffix(lod);
		names.push_back("geometry_" + suffix);
		types.emplace_back(LogicalTypeId::BLOB);
		names.push_back("geometry_properties_" + suffix);
		types.push_back(ColumnTypeUtils::ToDuckDBType(ColumnType::GeometryPropertiesStruct));
		names.push_back("material_" + suffix);
		types.push_back(varchar);
		names.push_back("texture_" + suffix);
		types.push_back(varchar);
	}
}

void RegisterAppearanceTableFunctions(ExtensionLoader &loader) {
	TableFunction materials("cityjson_materials", {LogicalType(LogicalTypeId::VARCHAR)}, AppearanceScan, MaterialsBind);
	materials.init_global = AppearanceInitGlobal;
	loader.RegisterFunction(materials);

	TableFunction textures("cityjson_textures", {LogicalType(LogicalTypeId::VARCHAR)}, AppearanceScan, TexturesBind);
	textures.init_global = AppearanceInitGlobal;
	loader.RegisterFunction(textures);

	TableFunction templates("cityjson_geometry_templates", {LogicalType(LogicalTypeId::VARCHAR)}, AppearanceScan,
	                        TemplatesBind);
	templates.init_global = AppearanceInitGlobal;
	loader.RegisterFunction(templates);
}

} // namespace cityjson
} // namespace duckdb

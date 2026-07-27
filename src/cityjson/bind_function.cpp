#include "cityjson/table_function.hpp"
#include "duckdb/common/string_util.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/column_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

#include <set>

namespace duckdb {
namespace cityjson {

CityJSONReadOptions ParseCityJSONReadOptions(const TableFunctionBindInput &input, const std::string &function_name) {
	CityJSONReadOptions options;

	for (auto &kv : input.named_parameters) {
		if (kv.first == "lod") {
			options.target_lod = LODTableUtils::NormalizeLOD(StringValue::Get(kv.second));
			options.use_wkb_encoding = true; // Enable WKB encoding when LOD is specified
		} else if (kv.first == "appearance") {
			auto mode = StringUtil::Lower(StringValue::Get(kv.second));
			if (mode == "sidecar") {
				options.sidecar_appearance = true;
			} else if (mode == "local") {
				options.sidecar_appearance = false;
			} else {
				throw BinderException(function_name + ": appearance must be 'local' or 'sidecar', got '" + mode + "'");
			}
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

void InferCityJSONColumns(CityJSONBindData &bind_data, CityJSONReader &reader, size_t sample_lines) {
	if (bind_data.target_lod.has_value()) {
		std::vector<CityJSONFeature> features;
		try {
			features = bind_data.streaming ? reader.ReadNFeatures(sample_lines) : FlattenChunks(bind_data.chunks);
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to infer LOD schema: " + std::string(e.what()));
		}

		auto lod_tables = LODTableUtils::InferLODTables(features, sample_lines);

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

CityJSONSourceFacts InspectCityJSONSource(CityJSONReader &reader, const CityJSONReadOptions &options) {
	// A probe bind_data, populated in the same order BindCityJSONReadRaw populates the
	// real one, so InferCityJSONColumns sees the state it expects. `streaming` stays
	// false deliberately: the streaming path infers from a sample, and a sample cannot
	// answer "which object types are in this file".
	CityJSONBindData probe;
	probe.streaming = false;
	probe.target_lod = options.target_lod;
	probe.use_wkb_encoding = options.use_wkb_encoding;

	try {
		probe.metadata = reader.ReadMetadata();
		probe.chunks = reader.ReadAllChunks();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read source file: " + std::string(e.what()));
	}
	InferCityJSONColumns(probe, reader, options.sample_lines);

	CityJSONSourceFacts facts;
	facts.columns = probe.columns;
	if (probe.metadata.metadata.has_value()) {
		facts.reference_system = probe.metadata.metadata.value().reference_system;
	}

	std::set<std::string> types;
	for (const auto &feature : probe.chunks.records) {
		for (const auto &entry : feature.city_objects) {
			types.insert(entry.second.type);
		}
	}
	facts.object_types.assign(types.begin(), types.end());

	// Interned, not counted from the header: a CityJSONSeq feature may carry definitions
	// the header never declared, and those need a sidecar just as much.
	const auto index = AppearanceIndex::Build(probe.metadata, probe.chunks.records);
	facts.has_materials = !index.materials.empty();
	facts.has_textures = !index.textures.empty();
	facts.has_geometry_templates =
	    probe.metadata.geometry_templates.has_value() && !probe.metadata.geometry_templates.value().Empty();
	return facts;
}

CityJSONBindData BindCityJSONReadRaw(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names,
                                     const std::string &function_name, CityJSONReader &reader, bool streaming) {
	CityJSONBindData result;

	if (input.inputs.empty()) {
		throw BinderException(function_name + " requires a file path");
	}
	result.file_name = StringValue::Get(input.inputs[0]);
	result.streaming = streaming;

	auto options = ParseCityJSONReadOptions(input, function_name);
	result.target_lod = options.target_lod;
	result.use_wkb_encoding = options.use_wkb_encoding;

	try {
		result.metadata = reader.ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read metadata: " + std::string(e.what()));
	}

	if (!streaming) {
		try {
			result.chunks = reader.ReadAllChunks();
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to read data: " + std::string(e.what()));
		}
		result.scan_plan = result.chunks.BuildScanPlan();
	}

	InferCityJSONColumns(result, reader, options.sample_lines);

	if (options.sidecar_appearance) {
		// Reading the whole file, not a sample: a definition used only by a feature in
		// the tail belongs in the dataset's sidecar just as much as a header one, and
		// omitting it would leave that feature's references unresolvable.
		auto all = reader.ReadAllChunks();
		result.appearance_index = AppearanceIndex::Build(result.metadata, all.records);
	}

	for (const auto &col : result.columns) {
		names.push_back(col.name);
		return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
	}

	return result;
}

unique_ptr<FunctionData> BindCityJSONRead(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names,
                                          const std::string &function_name,
                                          std::unique_ptr<CityJSONReader> reader, bool streaming) {
	auto result = make_uniq<CityJSONBindData>(
	    BindCityJSONReadRaw(context, input, return_types, names, function_name, *reader, streaming));
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
		// Sequence-only factory: never auto-detect to a full-CityJSON reader, otherwise
		// regular .city.json input would produce a silent empty result instead of an error.
		reader = OpenCityJSONSeqFile(context, file_name, options.sample_lines);
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to open CityJSONSeq file: " + std::string(e.what()));
	}

	return BindCityJSONRead(context, input, return_types, names, "read_cityjsonseq", std::move(reader), true);
}

void CityJSONPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                   vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<CityJSONBindData>();

	static const std::unordered_set<std::string> pushable_columns = {"id", "feature_id", "object_type"};

	for (auto it = filters.begin(); it != filters.end();) {
		auto &expr = *it;
		bool consumed = false;

		if (expr->type == ExpressionType::COMPARE_EQUAL) {
			auto &comp = expr->Cast<BoundComparisonExpression>();

			BoundColumnRefExpression *col_ref = nullptr;
			BoundConstantExpression *constant = nullptr;

			if (comp.left->type == ExpressionType::BOUND_COLUMN_REF &&
			    comp.right->type == ExpressionType::VALUE_CONSTANT) {
				col_ref = &comp.left->Cast<BoundColumnRefExpression>();
				constant = &comp.right->Cast<BoundConstantExpression>();
			} else if (comp.right->type == ExpressionType::BOUND_COLUMN_REF &&
			           comp.left->type == ExpressionType::VALUE_CONSTANT) {
				col_ref = &comp.right->Cast<BoundColumnRefExpression>();
				constant = &comp.left->Cast<BoundConstantExpression>();
			}

			if (col_ref && constant && constant->value.type() == LogicalType::VARCHAR) {
				// The column binding refers to an entry in get.GetColumnIds(),
				// not directly to get.names.
				if (col_ref->binding.table_index == get.table_index &&
				    col_ref->binding.column_index < get.GetColumnIds().size()) {
					idx_t schema_idx = get.GetColumnIds()[col_ref->binding.column_index].GetPrimaryIndex();
					if (schema_idx < bind_data.columns.size()) {
						const auto &column_name = bind_data.columns[schema_idx].name;
						if (pushable_columns.count(column_name) > 0) {
							bind_data.equality_filters.emplace_back(column_name,
							                                        constant->value.GetValue<std::string>());
							consumed = true;
						}
					}
				}
			}
		}

		if (consumed) {
			it = filters.erase(it);
		} else {
			++it;
		}
	}
}


} // namespace cityjson
} // namespace duckdb

#include "cityjson/table_function.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/column_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

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

static void InferSchema(CityJSONBindData &bind_data, CityJSONReader &reader, size_t sample_lines) {
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

	InferSchema(*result, *reader, options.sample_lines);

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
		reader = OpenAnyCityJSONFile(context, file_name, options.sample_lines);
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

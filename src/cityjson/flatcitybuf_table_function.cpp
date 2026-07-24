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
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include <fcb/generated/header_generated.h>
#include <algorithm>

namespace duckdb {
namespace cityjson {

namespace {

std::optional<fcb::Operator> ToFcbOperator(ExpressionType type, bool column_on_right) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL:
		return fcb::Operator::Eq;
	case ExpressionType::COMPARE_NOTEQUAL:
		return fcb::Operator::Ne;
	case ExpressionType::COMPARE_GREATERTHAN:
		return column_on_right ? fcb::Operator::Lt : fcb::Operator::Gt;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		return column_on_right ? fcb::Operator::Le : fcb::Operator::Ge;
	case ExpressionType::COMPARE_LESSTHAN:
		return column_on_right ? fcb::Operator::Gt : fcb::Operator::Lt;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		return column_on_right ? fcb::Operator::Ge : fcb::Operator::Le;
	default:
		return std::nullopt;
	}
}

// Types a DuckDB constant against the column's ON-DISK type, mirroring
// upstream's query_attributes.cpp make_value(). Getting this wrong doesn't
// throw -- the bytes are reinterpreted -- so every branch pulls the constant
// via the DuckDB getter that matches the on-disk type's own category.
std::optional<fcb::KeyValue> BuildKeyValue(const fcb::ColumnInfo &col, const Value &constant) {
	switch (static_cast<::ColumnType>(col.type)) {
	case ::ColumnType::Byte:
		return fcb::KeyValue::from_i8(static_cast<int8_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::UByte:
		return fcb::KeyValue::from_u8(static_cast<uint8_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::Bool:
		return fcb::KeyValue::from_bool(constant.GetValue<bool>());
	case ::ColumnType::Short:
		return fcb::KeyValue::from_i16(static_cast<int16_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::UShort:
		return fcb::KeyValue::from_u16(static_cast<uint16_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::Int:
		return fcb::KeyValue::from_i32(static_cast<int32_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::UInt:
		return fcb::KeyValue::from_u32(static_cast<uint32_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::Long:
		return fcb::KeyValue::from_i64(constant.GetValue<int64_t>());
	case ::ColumnType::ULong:
		return fcb::KeyValue::from_u64(static_cast<uint64_t>(constant.GetValue<int64_t>()));
	case ::ColumnType::Float:
		return fcb::KeyValue::from_f32(static_cast<float>(constant.GetValue<double>()));
	case ::ColumnType::Double:
		return fcb::KeyValue::from_f64(constant.GetValue<double>());
	case ::ColumnType::String:
		return fcb::KeyValue::from_string(fcb::KeyKind::String50, constant.ToString());
	case ::ColumnType::Json:
	case ::ColumnType::Binary:
		return fcb::KeyValue::from_string(fcb::KeyKind::String100, constant.ToString());
	default:
		return std::nullopt; // unsupported column type for pushdown -- leave unpushed
	}
}

} // namespace

void FlatCityBufPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data_p,
                                      vector<unique_ptr<Expression>> &filters) {
	auto &bind_data = bind_data_p->Cast<FlatCityBufBindData>();
	if (!bind_data.reader) {
		return;
	}
	auto indexed_columns = bind_data.reader->IndexedAttributeColumns();
	if (indexed_columns.empty()) {
		return;
	}

	fcb::AttrQuery conditions;

	// Deliberately never erase from `filters`, unlike CityJSONPushdownComplexFilter's
	// id/feature_id/object_type handling: this function's schema emits one row PER
	// CITYOBJECT, but select_attr/the post-filter both match at FEATURE granularity
	// ("does ANY CityObject in this feature satisfy the condition" -- see
	// FlatCityBufReader::MatchesAttrQueryPostFilter). Erasing the expression here
	// would skip DuckDB's own per-row check and silently emit every CityObject of a
	// matching feature, including ones that don't themselves satisfy the condition.
	// So this only NARROWS which features get read/decoded (a real index-assisted
	// skip); DuckDB still evaluates every surviving filter against every row exactly
	// as it would without this pushdown.
	for (auto &expr : filters) {
		if (expr->type < ExpressionType::COMPARE_EQUAL || expr->type > ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
			continue;
		}
		auto &comp = expr->Cast<BoundComparisonExpression>();

		BoundColumnRefExpression *col_ref = nullptr;
		BoundConstantExpression *constant = nullptr;
		bool column_on_right = false;

		if (comp.left->type == ExpressionType::BOUND_COLUMN_REF && comp.right->type == ExpressionType::VALUE_CONSTANT) {
			col_ref = &comp.left->Cast<BoundColumnRefExpression>();
			constant = &comp.right->Cast<BoundConstantExpression>();
		} else if (comp.right->type == ExpressionType::BOUND_COLUMN_REF &&
		           comp.left->type == ExpressionType::VALUE_CONSTANT) {
			col_ref = &comp.right->Cast<BoundColumnRefExpression>();
			constant = &comp.left->Cast<BoundConstantExpression>();
			column_on_right = true;
		}

		if (!col_ref || !constant || col_ref->binding.table_index != get.table_index ||
		    col_ref->binding.column_index >= get.GetColumnIds().size()) {
			continue;
		}
		idx_t schema_idx = get.GetColumnIds()[col_ref->binding.column_index].GetPrimaryIndex();
		if (schema_idx >= bind_data.columns.size()) {
			continue;
		}
		const auto &column_name = bind_data.columns[schema_idx].name;
		bool is_indexed =
		    std::find(indexed_columns.begin(), indexed_columns.end(), column_name) != indexed_columns.end();
		if (!is_indexed) {
			continue;
		}
		auto op = ToFcbOperator(expr->type, column_on_right);
		auto col_info = bind_data.reader->FindColumn(column_name);
		if (!op.has_value() || !col_info.has_value()) {
			continue;
		}
		auto key_value = BuildKeyValue(col_info.value(), constant->value);
		if (key_value.has_value()) {
			conditions.push_back({column_name, op.value(), key_value.value()});
		}
	}

	if (conditions.empty()) {
		return;
	}

	bind_data.reader->SetAttrQueryFilter(conditions, false);
	try {
		bind_data.chunks = bind_data.reader->ReadAllChunks();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to re-read FlatCityBuf with pushed-down attribute filter: " +
		                      std::string(e.what()));
	}
	bind_data.scan_plan = bind_data.chunks.BuildScanPlan();
}

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
	func.pushdown_complex_filter = FlatCityBufPushdownComplexFilter;

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

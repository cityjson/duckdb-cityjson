#include "cityjson/cityparquet_appearance.hpp"

#include "cityjson/json_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <set>

namespace duckdb {
namespace cityjson {

namespace {

//! material: `values` is a flat list of ids (or nulls); `value` is a single id.
void CollectMaterialIds(const json &theme, std::set<int64_t> &ids) {
	auto values = theme.find("values");
	if (values != theme.end() && values->is_array()) {
		for (const auto &entry : *values) {
			if (entry.is_number_integer()) {
				ids.insert(entry.get<int64_t>());
			}
		}
	}
	auto value = theme.find("value");
	if (value != theme.end() && value->is_number_integer()) {
		ids.insert(value->get<int64_t>());
	}
}

//! texture: `values` is per face, then per ring; each ring is [id, uv, uv, ...], so
//! only the ring's first element is a texture id. Collecting the whole ring would
//! sweep up UV references as though they were ids.
void CollectTextureIds(const json &theme, std::set<int64_t> &ids) {
	auto values = theme.find("values");
	if (values == theme.end() || !values->is_array()) {
		return;
	}
	for (const auto &face : *values) {
		if (!face.is_array()) {
			continue;
		}
		for (const auto &ring : face) {
			if (!ring.is_array() || ring.empty()) {
				continue;
			}
			if (ring[0].is_number_integer()) {
				ids.insert(ring[0].get<int64_t>());
			}
		}
	}
}

void AppearanceIdsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();

	UnifiedVectorFormat cell_format;
	UnifiedVectorFormat kind_format;
	args.data[0].ToUnifiedFormat(count, cell_format);
	args.data[1].ToUnifiedFormat(count, kind_format);
	const auto cells = UnifiedVectorFormat::GetData<string_t>(cell_format);
	const auto kinds = UnifiedVectorFormat::GetData<string_t>(kind_format);

	result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
	const auto list_type = LogicalType::LIST(LogicalType(LogicalTypeId::BIGINT));

	for (idx_t i = 0; i < count; i++) {
		const auto cell_idx = cell_format.sel->get_index(i);
		const auto kind_idx = kind_format.sel->get_index(i);
		if (!cell_format.validity.RowIsValid(cell_idx) || !kind_format.validity.RowIsValid(kind_idx)) {
			result.SetValue(i, Value(list_type));
			continue;
		}

		const auto kind = StringUtil::Lower(kinds[kind_idx].GetString());
		if (kind != "material" && kind != "texture") {
			throw InvalidInputException("cityjson_appearance_ids: kind must be 'material' or 'texture', got '%s'",
			                            kind);
		}

		json parsed;
		try {
			parsed = json::parse(cells[cell_idx].GetString());
		} catch (const std::exception &e) {
			throw InvalidInputException("cityjson_appearance_ids: cannot parse appearance cell: %s", e.what());
		}
		if (!parsed.is_object()) {
			result.SetValue(i, Value(list_type));
			continue;
		}

		// The outer dimension is the theme, a dynamic key set, so iterate rather than
		// look up a known name.
		std::set<int64_t> ids;
		for (const auto &entry : parsed.items()) {
			if (!entry.value().is_object()) {
				continue;
			}
			if (kind == "material") {
				CollectMaterialIds(entry.value(), ids);
			} else {
				CollectTextureIds(entry.value(), ids);
			}
		}

		duckdb::vector<Value> children;
		for (const auto id : ids) {
			children.push_back(Value::BIGINT(id));
		}
		result.SetValue(i, Value::LIST(LogicalType(LogicalTypeId::BIGINT), std::move(children)));
	}

	if (count == 1) {
		result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
	}
}

} // namespace

void RegisterAppearanceIdsFunction(ExtensionLoader &loader) {
	ScalarFunction func("cityjson_appearance_ids",
	                    {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                    LogicalType::LIST(LogicalType(LogicalTypeId::BIGINT)), AppearanceIdsFunction);
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

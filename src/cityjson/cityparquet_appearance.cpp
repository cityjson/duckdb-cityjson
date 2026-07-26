#include "cityjson/cityparquet_appearance.hpp"

#include "cityjson/json_utils.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <set>

namespace duckdb {
namespace cityjson {

namespace {

//! material: every integer leaf of `values` is an id, at whatever nesting depth.
//!
//! The nesting depth is not fixed: a MultiSurface nests one level (one id per surface),
//! a Solid two (per shell, per surface), a MultiSolid three. Reading only the top level
//! would return nothing at all for a Solid -- and an empty reference set makes vacuum
//! delete materials that are still in use.
void CollectMaterialIds(const json &node, std::set<int64_t> &ids) {
	if (node.is_number_integer()) {
		ids.insert(node.get<int64_t>());
		return;
	}
	if (!node.is_array()) {
		return;
	}
	for (const auto &child : node) {
		CollectMaterialIds(child, ids);
	}
}

//! texture: the innermost array is a *ring* -- `[id, uv, uv, ...]` -- of which only the
//! first element is a texture id; the rest are UV references. Above the ring sit
//! surface, and for a Solid or MultiSolid one or two further shell dimensions.
//!
//! So the depth above the ring varies, but the ring itself is recognisable: it is the
//! array whose own elements are scalars rather than arrays. Recurse while elements are
//! arrays; treat the first scalar-valued array as a ring. Assuming a fixed
//! face/ring depth, as an earlier version did, silently collected nothing for solids
//! (making vacuum delete live textures) and would collect UV indices as ids if the
//! nesting were shallower than assumed.
void CollectTextureIds(const json &node, std::set<int64_t> &ids) {
	if (!node.is_array() || node.empty()) {
		return;
	}
	if (node[0].is_array()) {
		for (const auto &child : node) {
			CollectTextureIds(child, ids);
		}
		return;
	}
	// A ring. Its first element is the texture id, or null for an untextured ring.
	if (node[0].is_number_integer()) {
		ids.insert(node[0].get<int64_t>());
	}
}

void CollectThemeIds(const json &theme, const std::string &kind, std::set<int64_t> &ids) {
	auto values = theme.find("values");
	if (values != theme.end()) {
		if (kind == "material") {
			CollectMaterialIds(*values, ids);
		} else {
			CollectTextureIds(*values, ids);
		}
	}
	// The whole-geometry form, material only.
	auto value = theme.find("value");
	if (kind == "material" && value != theme.end() && value->is_number_integer()) {
		ids.insert(value->get<int64_t>());
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
			CollectThemeIds(entry.value(), kind, ids);
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

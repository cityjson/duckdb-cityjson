#include "cityjson/cityparquet_appearance.hpp"

#include "cityjson/function_docs.hpp"
#include "cityjson/appearance_cell.hpp"
#include "cityjson/column_types.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <set>

namespace duckdb {
namespace cityjson {

namespace {

//! The two cells differ only in where their ids sit, so each kind contributes a
//! traits type and the two function bodies below are written once.
struct MaterialTraits {
	using Cell = MaterialCell;

	static Cell FromValue(const Value &value) {
		return MaterialCellFromValue(value);
	}

	static Value ToValue(const Cell &cell) {
		return MaterialCellValue(cell);
	}

	//! One id per WKB face; a face the source assigns no material to is null.
	static void CollectIds(const Cell &cell, std::set<int64_t> &ids) {
		for (const auto &theme : cell.themes) {
			for (const auto &id : theme.second) {
				if (id.has_value()) {
					ids.insert(id.value());
				}
			}
		}
	}

	static void ShiftIds(Cell &cell, int64_t offset) {
		for (auto &theme : cell.themes) {
			for (auto &id : theme.second) {
				if (id.has_value()) {
					id = id.value() + offset;
				}
			}
		}
	}
};

struct TextureTraits {
	using Cell = TextureCell;

	static Cell FromValue(const Value &value) {
		return TextureCellFromValue(value);
	}

	static Value ToValue(const Cell &cell) {
		return TextureCellValue(cell);
	}

	//! Per face, per ring. Only the ring's `id` is a sidecar reference; its `uv`
	//! pairs are coordinates. A bare ring references nothing.
	static void CollectIds(const Cell &cell, std::set<int64_t> &ids) {
		for (const auto &theme : cell.themes) {
			for (const auto &face : theme.second) {
				for (const auto &ring : face) {
					if (ring.id.has_value()) {
						ids.insert(ring.id.value());
					}
				}
			}
		}
	}

	//! Moving a `uv` coordinate would silently distort the texture, so only the id
	//! moves.
	static void ShiftIds(Cell &cell, int64_t offset) {
		for (auto &theme : cell.themes) {
			for (auto &face : theme.second) {
				for (auto &ring : face) {
					if (ring.id.has_value()) {
						ring.id = ring.id.value() + offset;
					}
				}
			}
		}
	}
};

//! Row-at-a-time over `Value`s: a cell is a nested MAP whose depth the flat vector
//! API would have to be walked by hand, and vacuum runs this once per row of a
//! sidecar scan rather than in an inner loop.
template <class Traits>
void CollectCellIds(DataChunk &args, Vector &result) {
	const auto count = args.size();
	const auto id_type = ListType::GetChildType(result.GetType());

	result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < count; i++) {
		const auto cell = args.data[0].GetValue(i);
		if (cell.IsNull()) {
			result.SetValue(i, Value(result.GetType()));
			continue;
		}
		// A set, so the ids come out deduplicated and ascending: the caller is an
		// `IN` list or a reference check, neither of which wants either kind of
		// noise.
		std::set<int64_t> ids;
		Traits::CollectIds(Traits::FromValue(cell), ids);

		duckdb::vector<Value> children;
		children.reserve(ids.size());
		for (const auto id : ids) {
			children.push_back(Value::BIGINT(id));
		}
		result.SetValue(i, Value::LIST(id_type, std::move(children)));
	}

	if (count == 1) {
		result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
	}
}

template <class Traits>
void ShiftCellIds(DataChunk &args, Vector &result) {
	const auto count = args.size();

	result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < count; i++) {
		const auto cell = args.data[0].GetValue(i);
		const auto offset = args.data[1].GetValue(i);
		if (cell.IsNull() || offset.IsNull()) {
			result.SetValue(i, Value(result.GetType()));
			continue;
		}
		auto parsed = Traits::FromValue(cell);
		Traits::ShiftIds(parsed, offset.GetValue<int64_t>());
		result.SetValue(i, Traits::ToValue(parsed));
	}

	if (count == 1) {
		result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
	}
}

//! True when `type` is the material cell type; the caller has already established
//! that it is one of the two, so anything else is a texture cell.
bool IsMaterialCell(const LogicalType &type) {
	return type == ColumnTypeUtils::ToDuckDBType(ColumnType::MaterialMap);
}

void AppearanceIdsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (IsMaterialCell(args.data[0].GetType())) {
		CollectCellIds<MaterialTraits>(args, result);
	} else {
		CollectCellIds<TextureTraits>(args, result);
	}
}

void ShiftAppearanceIdsFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	if (IsMaterialCell(args.data[0].GetType())) {
		ShiftCellIds<MaterialTraits>(args, result);
	} else {
		ShiftCellIds<TextureTraits>(args, result);
	}
}

/**
 * Resolve the cell kind from the argument's type.
 *
 * The two cell types are both MAPs, and DuckDB costs a MAP-to-MAP implicit cast on
 * the type id alone -- so two registered overloads, one per cell type, tie at cost
 * zero and every call is "could not choose a best candidate". The argument is
 * therefore declared ANY and the kind is resolved here, which is the same contract
 * (the type says which) with an error message that names both accepted types.
 */
const LogicalType &BindCellType(ScalarFunction &bound_function, const Expression &argument) {
	const auto &cell_type = argument.return_type;
	if (cell_type != ColumnTypeUtils::ToDuckDBType(ColumnType::MaterialMap) &&
	    cell_type != ColumnTypeUtils::ToDuckDBType(ColumnType::TextureMap)) {
		throw BinderException("%s: expected a %s (material) or %s (texture) cell, got %s", bound_function.name,
		                      ColumnTypeUtils::ToString(ColumnType::MaterialMap),
		                      ColumnTypeUtils::ToString(ColumnType::TextureMap), cell_type.ToString());
	}
	bound_function.arguments[0] = cell_type;
	return cell_type;
}

unique_ptr<FunctionData> BindAppearanceIds(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	BindCellType(bound_function, *arguments[0]);
	return nullptr;
}

unique_ptr<FunctionData> BindShiftAppearanceIds(ClientContext &context, ScalarFunction &bound_function,
                                                vector<unique_ptr<Expression>> &arguments) {
	// Shifting rewrites ids in place, so the result is the cell it was handed.
	bound_function.return_type = BindCellType(bound_function, *arguments[0]);
	return nullptr;
}

} // namespace

void RegisterAppearanceIdsFunction(ExtensionLoader &loader) {
	const auto id_list = LogicalType::LIST(LogicalType::BIGINT);

	ScalarFunction ids("cityjson_appearance_ids", {LogicalType::ANY}, id_list, AppearanceIdsFunction,
	                   BindAppearanceIds);
	RegisterDocumented(loader, std::move(ids),
	                   {{"cell"},
	                    "Returns the distinct appearance ids a material_lod* or texture_lod* cell references, as an "
	                    "ascending list.",
	                    "SELECT cityjson_appearance_ids(material_lod3_0) FROM "
	                    "read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar') "
	                    "WHERE material_lod3_0 IS NOT NULL LIMIT 1;",
	                    {"cityjson", "appearance"}});

	// The declared return type is a placeholder: the bind replaces it with the cell
	// type it was actually handed.
	ScalarFunction shift("cityjson_shift_appearance_ids", {LogicalType::ANY, LogicalType::BIGINT},
	                     ColumnTypeUtils::ToDuckDBType(ColumnType::MaterialMap), ShiftAppearanceIdsFunction,
	                     BindShiftAppearanceIds);
	RegisterDocumented(loader, std::move(shift),
	                   {{"cell", "offset"},
	                    "Adds a constant to every appearance id in a material_lod* or texture_lod* cell; the "
	                    "renumbering step that cityparquet_merge and insert_cityjson generate.",
	                    "SELECT cityjson_shift_appearance_ids(material_lod3_0, 10) FROM "
	                    "read_cityjsonseq('test/data/railway_appearance.city.jsonl', appearance := 'sidecar') "
	                    "WHERE material_lod3_0 IS NOT NULL LIMIT 1;",
	                    {"cityjson", "appearance"}});
}

} // namespace cityjson
} // namespace duckdb

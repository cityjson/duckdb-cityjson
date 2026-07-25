#include "cityjson/wkb_extent.hpp"

#include "cityjson/wkb_decoder.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {
namespace cityjson {

namespace {

struct Extent3D {
	double min_x = std::numeric_limits<double>::infinity();
	double min_y = std::numeric_limits<double>::infinity();
	double min_z = std::numeric_limits<double>::infinity();
	double max_x = -std::numeric_limits<double>::infinity();
	double max_y = -std::numeric_limits<double>::infinity();
	double max_z = -std::numeric_limits<double>::infinity();

	bool Empty() const {
		return min_x > max_x;
	}

	void Add(double x, double y, double z) {
		min_x = std::min(min_x, x);
		min_y = std::min(min_y, y);
		min_z = std::min(min_z, z);
		max_x = std::max(max_x, x);
		max_y = std::max(max_y, y);
		max_z = std::max(max_z, z);
	}
};

// The decoder emits nested arrays whose leaves are [x, y, z] coordinate arrays.
// Recursing to the leaves rather than switching on geometry type covers every type
// the decoder supports -- MultiPoint through PolyhedralSurfaceZ and
// GeometryCollectionZ -- with no per-type branch to keep in sync as the decoder grows.
void Accumulate(const json &node, Extent3D &extent) {
	if (!node.is_array() || node.empty()) {
		return;
	}
	if (node[0].is_number()) {
		if (node.size() < 3) {
			throw InvalidInputException("cityjson_wkb_extent: coordinate array with fewer than 3 values");
		}
		extent.Add(node[0].get<double>(), node[1].get<double>(), node[2].get<double>());
		return;
	}
	for (const auto &child : node) {
		Accumulate(child, extent);
	}
}

void WKBExtentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto &input = args.data[0];

	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);
	const auto blobs = UnifiedVectorFormat::GetData<string_t>(format);

	const auto extent_type = ExtentType();
	result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);

	for (idx_t i = 0; i < count; i++) {
		const auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			result.SetValue(i, Value(extent_type));
			continue;
		}

		const auto &blob = blobs[idx];
		Extent3D extent;
		try {
			auto decoded = WKBDecoder::Decode(reinterpret_cast<const uint8_t *>(blob.GetData()), blob.GetSize());
			Accumulate(decoded.boundaries, extent);
		} catch (const InvalidInputException &) {
			throw;
		} catch (const std::exception &e) {
			throw InvalidInputException("cityjson_wkb_extent: cannot decode WKB: %s", e.what());
		}

		if (extent.Empty()) {
			// Structurally valid WKB carrying no coordinates (an empty collection,
			// say) has no extent. NULL is the honest answer; zeros would be a lie
			// that silently corrupts any bbox union it takes part in.
			result.SetValue(i, Value(extent_type));
			continue;
		}

		child_list_t<Value> fields;
		fields.emplace_back("min_x", Value::DOUBLE(extent.min_x));
		fields.emplace_back("min_y", Value::DOUBLE(extent.min_y));
		fields.emplace_back("min_z", Value::DOUBLE(extent.min_z));
		fields.emplace_back("max_x", Value::DOUBLE(extent.max_x));
		fields.emplace_back("max_y", Value::DOUBLE(extent.max_y));
		fields.emplace_back("max_z", Value::DOUBLE(extent.max_z));
		result.SetValue(i, Value::STRUCT(std::move(fields)));
	}

	if (count == 1) {
		result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
	}
}

} // namespace

LogicalType ExtentType() {
	// LogicalType(LogicalTypeId::DOUBLE), not LogicalType::DOUBLE: the latter is a
	// `static constexpr` member, and emplace_back binds a forwarding reference to it,
	// which ODR-uses it and emits a comdat definition that collides at link time with
	// DuckDB's own strong definition in ub_duckdb_common.cpp.
	child_list_t<LogicalType> children;
	children.emplace_back("min_x", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("min_y", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("min_z", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("max_x", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("max_y", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("max_z", LogicalType(LogicalTypeId::DOUBLE));
	return LogicalType::STRUCT(children);
}

void RegisterWKBExtentFunction(ExtensionLoader &loader) {
	ScalarFunction func("cityjson_wkb_extent", {LogicalType::BLOB}, ExtentType(), WKBExtentFunction);
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

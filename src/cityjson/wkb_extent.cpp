#include "cityjson/wkb_extent.hpp"

#include "cityjson/function_docs.hpp"
#include "cityjson/wkb_decoder.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/function/scalar_function.hpp"

#include <algorithm>
#include <limits>

namespace duckdb {
namespace cityjson {

//! The WKB type name with the CityParquet " Z" suffix, e.g. "PolyhedralSurface Z".
//! Feeds the footer's `city.columns[].geometry_types`, which is also what decides
//! whether a column may legally be declared in the GeoParquet `geo` object.
//!
//! Deliberately OUTSIDE the anonymous namespace below: the footer writers
//! (cityparquet_write.cpp) call it through the declaration in wkb_extent.hpp, and
//! internal linkage here would leave that declaration unresolved at link time.
const char *WKBTypeName(uint32_t code) {
	switch (code) {
	case 1001:
		return "Point Z";
	case 1002:
		return "LineString Z";
	case 1003:
		return "Polygon Z";
	case 1004:
		return "MultiPoint Z";
	case 1005:
		return "MultiLineString Z";
	case 1006:
		return "MultiPolygon Z";
	case 1007:
		return "GeometryCollection Z";
	case 1015:
		return "PolyhedralSurface Z";
	case 1016:
		return "TIN Z";
	case 1017:
		return "Triangle Z";
	default:
		return nullptr;
	}
}

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

void WKBGeometryTypeFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto blobs = args.data[0].Values<string_t>();
	auto writer = FlatVector::Writer<string_t>(result, count);
	for (idx_t i = 0; i < count; i++) {
		const auto entry = blobs[i];
		if (!entry.IsValid()) {
			writer.WriteNull();
			continue;
		}
		const auto blob = entry.GetValue();
		// Only the 5-byte header is needed: byte order, then the type code. No decode.
		if (blob.GetSize() < 5) {
			throw InvalidInputException("cityjson_wkb_geometry_type: WKB shorter than its 5-byte header");
		}
		const auto *data = reinterpret_cast<const uint8_t *>(blob.GetData());
		const bool little_endian = data[0] == 1;
		uint32_t code = 0;
		if (little_endian) {
			code = static_cast<uint32_t>(data[1]) | (static_cast<uint32_t>(data[2]) << 8) |
			       (static_cast<uint32_t>(data[3]) << 16) | (static_cast<uint32_t>(data[4]) << 24);
		} else {
			code = (static_cast<uint32_t>(data[1]) << 24) | (static_cast<uint32_t>(data[2]) << 16) |
			       (static_cast<uint32_t>(data[3]) << 8) | static_cast<uint32_t>(data[4]);
		}
		const auto *name = WKBTypeName(code);
		if (name == nullptr) {
			throw InvalidInputException("cityjson_wkb_geometry_type: unsupported WKB type code %u", code);
		}
		writer.WriteValue(string_t(name));
	}
}

void WKBExtentFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto count = args.size();
	auto blobs = args.data[0].Values<string_t>();
	auto writer = FlatVector::Writer<VectorStructType<double, double, double, double, double, double>>(result, count);
	for (idx_t i = 0; i < count; i++) {
		const auto entry = blobs[i];
		if (!entry.IsValid()) {
			writer.WriteNull();
			continue;
		}

		const auto blob = entry.GetValue();
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
			writer.WriteNull();
			continue;
		}

		// Children in ExtentType() order: xmin, ymin, zmin, xmax, ymax, zmax.
		writer.WriteValue([&](auto &xmin, auto &ymin, auto &zmin, auto &xmax, auto &ymax, auto &zmax) {
			xmin.WriteValue(extent.min_x);
			ymin.WriteValue(extent.min_y);
			zmin.WriteValue(extent.min_z);
			xmax.WriteValue(extent.max_x);
			ymax.WriteValue(extent.max_y);
			zmax.WriteValue(extent.max_z);
		});
	}
}

} // namespace

LogicalType ExtentType() {
	// LogicalType(LogicalTypeId::DOUBLE), not LogicalType::DOUBLE: the latter is a
	// `static constexpr` member, and emplace_back binds a forwarding reference to it,
	// which ODR-uses it and emits a comdat definition that collides at link time with
	// DuckDB's own strong definition in ub_duckdb_common.cpp.
	child_list_t<LogicalType> children;
	children.emplace_back("xmin", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("ymin", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("zmin", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("xmax", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("ymax", LogicalType(LogicalTypeId::DOUBLE));
	children.emplace_back("zmax", LogicalType(LogicalTypeId::DOUBLE));
	return LogicalType::STRUCT(children);
}

void RegisterWKBExtentFunction(ExtensionLoader &loader) {
	ScalarFunction func("cityjson_wkb_extent", {LogicalType::BLOB}, ExtentType(), WKBExtentFunction);
	RegisterDocumented(loader, std::move(func),
	                   {{"wkb"},
	                    "Returns the 3D extent (xmin, ymin, zmin, xmax, ymax, zmax) of a WKB geometry, including the "
	                    "PolyhedralSurface Z solids that DuckDB spatial rejects.",
	                    "SELECT cityjson_wkb_extent(geometry_lod2_2).zmax FROM "
	                    "read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl') "
	                    "WHERE geometry_lod2_2 IS NOT NULL LIMIT 1;",
	                    {"geometry"}});

	ScalarFunction type_name("cityjson_wkb_geometry_type", {LogicalType::BLOB}, LogicalType(LogicalTypeId::VARCHAR),
	                         WKBGeometryTypeFunction);
	RegisterDocumented(loader, std::move(type_name),
	                   {{"wkb"},
	                    "Returns the geometry type name of a WKB blob, such as 'MultiPolygon Z' or "
	                    "'PolyhedralSurface Z', without decoding its coordinates.",
	                    "SELECT DISTINCT cityjson_wkb_geometry_type(geometry_lod2_2) FROM "
	                    "read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl') "
	                    "WHERE geometry_lod2_2 IS NOT NULL;",
	                    {"geometry"}});
}

} // namespace cityjson
} // namespace duckdb

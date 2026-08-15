#include "cityjson/geoparquet_table_function.hpp"
#include "cityjson/table_function.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/crs_projjson.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/error.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/cityparquet_package.hpp"
#include "cityjson/wkb_encoder.hpp"
#include "cityjson/wkb_extent.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <limits>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

namespace {

// CityJSON geometry types that map to GeoParquet-legal WKB (types 1001–1007, §13.3):
// MultiPoint→MultiPointZ, MultiLineString→MultiLineStringZ,
// MultiSurface/CompositeSurface→MultiPolygonZ. Solid-family types map to
// PolyhedralSurfaceZ / GeometryCollectionZ and are excluded.
std::string GeoParquetTypeName(const std::string &cj_type, GeometryEncoding encoding) {
	// `geo` describes WKB columns. Legality is a property of the physical encoding
	// first and the CityJSON type second: a MultiSurface is legal as WKB but the
	// same type encoded arrow-native is a nested LIST of indices no GeoParquet
	// reader can decode, so no column using it may be declared here.
	if (encoding != GeometryEncoding::Wkb) {
		return "";
	}
	if (cj_type == "MultiSurface" || cj_type == "CompositeSurface") {
		return "MultiPolygon Z";
	}
	if (cj_type == "MultiPoint") {
		return "MultiPoint Z";
	}
	if (cj_type == "MultiLineString") {
		return "MultiLineString Z";
	}
	return ""; // GeoParquet-illegal (Solid family, or non-surface)
}

// Parse a normalised LoD string ("2", "2.2", "10") to a comparable number so the
// highest LoD is chosen by value, not by lexical column-name order (where
// "lod2_2" would sort after "lod10").
double LodValue(const std::string &normalised_lod) {
	try {
		return std::stod(normalised_lod);
	} catch (...) {
		return -1.0;
	}
}

// Build the GeoParquet `geo` JSON, or return nullopt when no column qualifies.
// `lod_types` maps a normalised LoD string to the set of CityJSON geometry types
// seen at that LoD. Throws when a present CRS cannot be resolved to PROJJSON.
std::optional<std::string> BuildGeoMetadata(const std::optional<std::string> &reference_system,
                                            const std::map<std::string, std::set<std::string>> &lod_types,
                                            GeometryEncoding encoding) {
	// Determine the GeoParquet-legal columns first. A column is legal only if
	// every type seen at its LoD maps to a GeoParquet-legal WKB type.
	json columns = json::object();
	std::string primary_column;
	double primary_lod = -1.0;
	for (const auto &[lod, types] : lod_types) {
		std::set<std::string> geometry_types;
		bool all_legal = !types.empty();
		for (const auto &t : types) {
			std::string name = GeoParquetTypeName(t, encoding);
			if (name.empty()) {
				all_legal = false;
				break;
			}
			geometry_types.insert(name);
		}
		if (!all_legal) {
			continue;
		}
		std::string col_name = "geometry_" + LODTableUtils::FormatLODAsColumnSuffix(lod);
		json col;
		col["encoding"] = "WKB";
		col["geometry_types"] = json(geometry_types); // sorted, deduplicated
		col["edges"] = "planar";
		columns[col_name] = std::move(col);
		if (LodValue(lod) >= primary_lod) {
			primary_lod = LodValue(lod);
			primary_column = col_name;
		}
	}

	if (columns.empty()) {
		// Solid-only (or geometry-less) dataset — no GeoParquet geo, and no CRS
		// resolution attempted, so an unknown CRS here does not error.
		return std::nullopt;
	}

	// Resolve the CRS once (shared by every column). A referenceSystem that we
	// cannot resolve is a hard error, never a silent omission (§13.3). Absent CRS
	// is written as an explicit `null` rather than omitted, so it does not falsely
	// assert GeoParquet's CRS84 default on a city model of unknown CRS.
	json crs_value = nullptr;
	if (reference_system.has_value() && !reference_system->empty()) {
		auto projjson = ProjjsonForReferenceSystem(reference_system.value());
		if (!projjson.has_value()) {
			throw InvalidInputException("cityjson_geoparquet_geo: cannot resolve CRS '" + reference_system.value() +
			                            "' to PROJJSON (unknown EPSG code)");
		}
		crs_value = json_utils::ParseJson(projjson.value());
	}
	for (auto &entry : columns.items()) {
		entry.value()["crs"] = crs_value;
	}

	json geo;
	geo["version"] = "1.1.0";
	geo["primary_column"] = primary_column;
	geo["columns"] = std::move(columns);
	return geo.dump();
}

// WKB type name for a CityJSON geometry type, solids included -- the city.columns
// geometry_types vocabulary, which is encoding-independent (spec 05-metadata.mdx).
// nullptr for a type WKB has no name for.
const char *CityColumnTypeName(const std::string &cj_type) {
	try {
		return WKBTypeName(static_cast<uint32_t>(WKBEncoder::GetOGCType(cj_type)));
	} catch (const CityJSONError &) {
		return nullptr;
	}
}

// Build the `city` footer JSON for the COPY path (spec 05-metadata.mdx, "The city
// object"). Unlike `geo`, EVERY LoD column is declared, Solid-family included, and
// `encoding` records the real physical encoding. An unresolvable CRS leaves `crs`
// null here; when any column is GeoParquet-legal BuildGeoMetadata has already
// thrown for it, so this lenience only ever applies to solid-only files.
std::string BuildCityMetadata(const std::optional<std::string> &reference_system,
                              const std::map<std::string, std::set<std::string>> &lod_types,
                              GeometryEncoding encoding, const std::vector<std::string> &attributes) {
	json crs_value = nullptr;
	if (reference_system.has_value() && !reference_system->empty()) {
		auto projjson = ProjjsonForReferenceSystem(reference_system.value());
		if (projjson.has_value()) {
			crs_value = json_utils::ParseJson(projjson.value());
		}
	}
	json columns = json::array();
	std::string primary;
	double primary_lod = -1.0;
	for (const auto &[lod, types] : lod_types) {
		std::set<std::string> geometry_types;
		for (const auto &t : types) {
			const auto *name = CityColumnTypeName(t);
			if (name != nullptr) {
				geometry_types.insert(name);
			}
		}
		if (geometry_types.empty()) {
			continue;
		}
		std::string col_name = "geometry_" + LODTableUtils::FormatLODAsColumnSuffix(lod);
		json col;
		col["name"] = col_name;
		col["encoding"] = encoding == GeometryEncoding::ArrowNative ? "CityParquetArrowNative-v1" : "WKB";
		col["geometry_types"] = json(geometry_types);
		col["crs"] = crs_value;
		col["orientation_3d"] = "right-handed";
		col["edges"] = "planar";
		columns.push_back(std::move(col));
		if (LodValue(lod) >= primary_lod) {
			primary_lod = LodValue(lod);
			primary = col_name;
		}
	}
	json city;
	city["version"] = CITYPARQUET_VERSION;
	city["crs"] = crs_value;
	if (!columns.empty()) {
		city["columns"] = std::move(columns);
		city["primary_column"] = primary;
	}
	city["attributes"] = json(attributes);
	return city.dump();
}

struct GeoBindData : public TableFunctionData {
	std::string file_name;
	//! The encoding the two footer objects below were built for. Both differ by it --
	//! `geo` declares no column at all under arrow-native -- so it is half of this bind's
	//! identity, not an incidental parameter.
	GeometryEncoding geometry_encoding = GeometryEncoding::Wkb;
	std::optional<std::string> geo; // nullopt -> emit SQL NULL
	//! The `city` object is REQUIRED on every CityParquet file, so unlike `geo` it is
	//! always non-empty.
	std::string city;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<GeoBindData>();
		result->file_name = file_name;
		result->geometry_encoding = geometry_encoding;
		result->geo = geo;
		result->city = city;
		return result;
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<GeoBindData>();
		// `geo` and `city` are derived deterministically from these two, so comparing the
		// inputs is sufficient and matches CityJSONBindData::Equals (bind_data.cpp).
		// file_name alone was not: two binds of one file at different encodings compared
		// equal, and a plan copy or a cached bind could then serve the wrong footer.
		return file_name == other.file_name && geometry_encoding == other.geometry_encoding;
	}
};

struct GeoGlobalState : public GlobalTableFunctionState {
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

unique_ptr<FunctionData> GeoBind(ClientContext &context, TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<GeoBindData>();
	result->file_name = StringValue::Get(input.inputs[0]);

	// Routed through the shared parser so the accepted spellings and the rejection
	// message stay identical to read_cityjson's.
	const auto encoding = ParseCityJSONReadOptions(input, "cityjson_geoparquet_geo").geometry_encoding;
	result->geometry_encoding = encoding;

	std::unique_ptr<CityJSONReader> reader;
	try {
		reader = OpenAnyCityJSONFile(context, result->file_name);
	} catch (const CityJSONError &e) {
		throw BinderException("cityjson_geoparquet_geo: failed to open file: " + std::string(e.what()));
	}

	std::optional<std::string> reference_system;
	try {
		auto meta = reader->ReadMetadata();
		if (meta.metadata.has_value()) {
			reference_system = meta.metadata->reference_system;
		}
	} catch (const CityJSONError &e) {
		throw BinderException("cityjson_geoparquet_geo: failed to read metadata: " + std::string(e.what()));
	}

	// Collect, per LoD, every CityJSON geometry type present. Legality must reflect
	// the whole dataset — a single Solid at a LoD makes that column non-legal — so
	// this scans all features rather than a sample.
	std::map<std::string, std::set<std::string>> lod_types;
	try {
		auto all = reader->ReadAllChunks();
		for (const auto &feature : all.records) {
			for (const auto &[obj_id, obj] : feature.city_objects) {
				for (const auto &geom : obj.geometry) {
					if (geom.lod.empty()) {
						continue; // lodless geometry gets no column (spec §9)
					}
					lod_types[LODTableUtils::NormalizeLOD(geom.lod)].insert(geom.type);
				}
			}
		}
	} catch (const CityJSONError &e) {
		throw BinderException("cityjson_geoparquet_geo: failed to read geometries: " + std::string(e.what()));
	}

	result->geo = BuildGeoMetadata(reference_system, lod_types, encoding);

	std::vector<std::string> attributes;
	for (const auto &col : reader->Columns()) {
		if (!IsReservedColumnName(col.name)) {
			attributes.push_back(col.name);
		}
	}
	result->city = BuildCityMetadata(reference_system, lod_types, encoding, attributes);

	return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR};
	names = {"geo", "city"};
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> GeoInitGlobal(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<GeoGlobalState>();
}

void GeoScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<GeoBindData>();
	auto &global_state = data.global_state->Cast<GeoGlobalState>();
	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}
	output.SetCardinality(1);
	if (bind_data.geo.has_value()) {
		output.data[0].SetValue(0, Value(bind_data.geo.value()));
	} else {
		output.data[0].SetValue(0, Value(LogicalType::VARCHAR)); // NULL
	}
	output.data[1].SetValue(0, Value(bind_data.city));
	global_state.done = true;
}

} // namespace

void RegisterGeoParquetTableFunctions(ExtensionLoader &loader) {
	TableFunction func("cityjson_geoparquet_geo", {LogicalType::VARCHAR}, GeoScan, GeoBind);
	// 'wkb' (default) or 'arrow-native': an arrow-native column is never declared.
	func.named_parameters["geometry_encoding"] = LogicalType::VARCHAR;
	func.init_global = GeoInitGlobal;
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

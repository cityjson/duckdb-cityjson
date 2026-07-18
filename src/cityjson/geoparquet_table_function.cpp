#include "cityjson/geoparquet_table_function.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/crs_projjson.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/error.hpp"
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
std::string GeoParquetTypeName(const std::string &cj_type) {
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
                                            const std::map<std::string, std::set<std::string>> &lod_types) {
	// Determine the GeoParquet-legal columns first. A column is legal only if
	// every type seen at its LoD maps to a GeoParquet-legal WKB type.
	json columns = json::object();
	std::string primary_column;
	double primary_lod = -1.0;
	for (const auto &[lod, types] : lod_types) {
		std::set<std::string> geometry_types;
		bool all_legal = !types.empty();
		for (const auto &t : types) {
			std::string name = GeoParquetTypeName(t);
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
		col["cityparquet:orientation"] = "right-handed";
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

struct GeoBindData : public TableFunctionData {
	std::string file_name;
	std::optional<std::string> geo; // nullopt -> emit SQL NULL

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<GeoBindData>();
		result->file_name = file_name;
		result->geo = geo;
		return result;
	}
	bool Equals(const FunctionData &other) const override {
		return file_name == other.Cast<GeoBindData>().file_name;
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

	result->geo = BuildGeoMetadata(reference_system, lod_types);

	return_types = {LogicalType::VARCHAR};
	names = {"geo"};
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
	global_state.done = true;
}

} // namespace

void RegisterGeoParquetTableFunctions(ExtensionLoader &loader) {
	TableFunction func("cityjson_geoparquet_geo", {LogicalType::VARCHAR}, GeoScan, GeoBind);
	func.init_global = GeoInitGlobal;
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

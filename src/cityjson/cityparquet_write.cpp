#include "cityjson/cityparquet_write.hpp"

#include "cityjson/cityparquet_package.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/crs_projjson.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <set>

namespace duckdb {
namespace cityjson {

namespace {

constexpr const char *CITYPARQUET_VERSION = "0.1.0-draft";

//! WKB type names GeoParquet 1.1 permits (codes 1001-1007). A column carrying anything
//! else -- any solid-family geometry -- MUST NOT be declared in `geo`: a strict reader
//! that eagerly decodes every declared column rejects the WHOLE file on one it cannot
//! parse, taking a perfectly good footprint column down with it.
bool GeoParquetLegal(const std::string &type_name) {
	static const std::set<std::string> legal = {"Point Z",      "LineString Z",   "Polygon Z",
	                                            "MultiPoint Z", "MultiLineString Z", "MultiPolygon Z",
	                                            "GeometryCollection Z"};
	return legal.count(type_name) > 0;
}

struct ColumnFacts {
	std::string name;
	std::set<std::string> geometry_types;
	bool has_extent = false;
	double min_x = 0, min_y = 0, min_z = 0, max_x = 0, max_y = 0, max_z = 0;

	bool Legal() const {
		if (geometry_types.empty()) {
			return false;
		}
		for (const auto &type : geometry_types) {
			if (!GeoParquetLegal(type)) {
				return false;
			}
		}
		return true;
	}
};

struct WriteBindData : public TableFunctionData {
	std::string schema;
	std::string directory;
	std::string crs;
	std::string source_format;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<WriteBindData>();
		result->schema = schema;
		result->directory = directory;
		result->crs = crs;
		result->source_format = source_format;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<WriteBindData>();
		return schema == o.schema && directory == o.directory;
	}
};

struct WrittenFile {
	std::string file;
	std::string action;
	int64_t rows = 0;
	int64_t bytes = 0;
};

//! The dataset-level view metadata.json carries, accumulated as the files are written.
//!
//! The footer answers for the one file it lives in; the STAC Item answers for the
//! package, so every one of these is a union or a sum across files rather than a copy of
//! any single footer. `city3d:lods` is the standing example — a package's LoD set is the
//! union over its tables, and no table's footer holds it.
struct PackageInventory {
	std::set<std::string> lods;
	std::set<std::string> co_types;
	std::set<std::string> attributes;
	int64_t city_objects = 0;
	int64_t materials = 0;
	int64_t textures = 0;
	bool semantic_surfaces = false;
	bool has_extent = false;
	double min_x = 0, min_y = 0, min_z = 0, max_x = 0, max_y = 0, max_z = 0;

	void Cover(const ColumnFacts &facts) {
		if (!facts.has_extent) {
			return;
		}
		if (!has_extent) {
			has_extent = true;
			min_x = facts.min_x;
			min_y = facts.min_y;
			min_z = facts.min_z;
			max_x = facts.max_x;
			max_y = facts.max_y;
			max_z = facts.max_z;
			return;
		}
		min_x = std::min(min_x, facts.min_x);
		min_y = std::min(min_y, facts.min_y);
		min_z = std::min(min_z, facts.min_z);
		max_x = std::max(max_x, facts.max_x);
		max_y = std::max(max_y, facts.max_y);
		max_z = std::max(max_z, facts.max_z);
	}
};

struct WriteGlobalState : public GlobalTableFunctionState {
	std::vector<WrittenFile> files;
	idx_t offset = 0;
	bool done = false;
	idx_t MaxThreads() const override {
		return 1;
	}
};

std::string Literal(const std::string &text) {
	return KeywordHelper::WriteQuoted(text, '\'');
}

//! Run a query on the internal connection, throwing its error rather than swallowing it.
unique_ptr<MaterializedQueryResult> Run(Connection &connection, const std::string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("cityparquet_write: %s\n(while running: %s)", result->GetError(), sql);
	}
	return result;
}

//! Fold one object table into the package-level inventory.
void CollectInventory(Connection &connection, const std::string &schema, const std::string &table, int64_t rows,
                      const std::vector<ColumnFacts> &facts, const std::vector<std::string> &attributes,
                      PackageInventory &inventory) {
	inventory.city_objects += rows;
	for (const auto &attribute : attributes) {
		inventory.attributes.insert(attribute);
	}
	for (const auto &column : facts) {
		inventory.Cover(column);
		// The LoD is recovered from the column name, which is where CityParquet keeps it.
		// Only columns that some row actually populates reach here, so the union is of
		// LoDs the package really carries rather than of columns it happens to declare.
		const auto suffix = column.name.substr(std::string("geometry_").size());
		auto lod = LODTableUtils::ParseLODFromSuffix(suffix);
		if (!lod.empty()) {
			inventory.lods.insert(lod);
		}
	}

	// The source type vocabulary, per the specification: the STAC Item mirrors the
	// source model's inventory, not the by-module routing that put the rows in this file.
	auto types = Run(connection, "SELECT DISTINCT object_type FROM " + QualifiedName(schema, table) +
	                                 " WHERE object_type IS NOT NULL");
	for (idx_t row = 0; row < types->RowCount(); row++) {
		inventory.co_types.insert(types->GetValue(0, row).ToString());
	}

	if (inventory.semantic_surfaces) {
		return;
	}
	for (const auto &column : facts) {
		const auto properties =
		    "geometry_properties_" + column.name.substr(std::string("geometry_").size());
		auto present = Run(connection, "SELECT COUNT(*) FROM " + QualifiedName(schema, table) + " WHERE " +
		                                   KeywordHelper::WriteOptionallyQuoted(properties) +
		                                   ".surfaces IS NOT NULL");
		if (present->GetValue(0, 0).GetValue<int64_t>() > 0) {
			inventory.semantic_surfaces = true;
			return;
		}
	}
}

//! The geometry_templates sidecar's contribution to the package inventory: its LoDs and
//! whether any template carries semantic surfaces. Templates are stored in local,
//! unplaced coordinates, so they contribute no extent.
void CollectTemplateInventory(Connection &connection, ClientContext &context, const std::string &schema,
                              const std::string &table, PackageInventory &inventory) {
	for (const auto &column : GeometryLodColumns(context, schema, table)) {
		const auto suffix = column.substr(std::string("geometry_").size());
		auto lod = LODTableUtils::ParseLODFromSuffix(suffix);
		if (!lod.empty()) {
			inventory.lods.insert(lod);
		}
		if (inventory.semantic_surfaces) {
			continue;
		}
		auto present = Run(connection, "SELECT COUNT(*) FROM " + QualifiedName(schema, table) + " WHERE " +
		                                   KeywordHelper::WriteOptionallyQuoted("geometry_properties_" + suffix) +
		                                   ".surfaces IS NOT NULL");
		if (present->GetValue(0, 0).GetValue<int64_t>() > 0) {
			inventory.semantic_surfaces = true;
		}
	}
}

//! Per geometry column: the WKB types actually present and the column's extent. Both are
//! recomputed from the data every time, never carried: GeoParquet legality flips in both
//! directions under mutation, so a stale `geo` can declare a column that now holds a
//! solid, which is data-corruption-adjacent rather than merely untidy.
std::vector<ColumnFacts> CollectFacts(Connection &connection, ClientContext &context, const std::string &schema,
                                      const std::string &table) {
	std::vector<ColumnFacts> facts;
	for (const auto &column : GeometryLodColumns(context, schema, table)) {
		ColumnFacts entry;
		entry.name = column;
		const auto quoted = KeywordHelper::WriteOptionallyQuoted(column);
		auto result = Run(connection, "SELECT DISTINCT cityjson_wkb_geometry_type(" + quoted + ") FROM " +
		                                  QualifiedName(schema, table) + " WHERE " + quoted + " IS NOT NULL");
		for (idx_t row = 0; row < result->RowCount(); row++) {
			auto value = result->GetValue(0, row);
			if (!value.IsNull()) {
				entry.geometry_types.insert(value.ToString());
			}
		}
		if (entry.geometry_types.empty()) {
			continue; // the column exists but no row populates it
		}
		auto extent = Run(connection, "SELECT min(e.min_x), min(e.min_y), min(e.min_z), max(e.max_x), max(e.max_y), "
		                              "max(e.max_z) FROM (SELECT cityjson_wkb_extent(" +
		                                  quoted + ") AS e FROM " + QualifiedName(schema, table) + " WHERE " + quoted +
		                                  " IS NOT NULL) t");
		if (extent->RowCount() == 1 && !extent->GetValue(0, 0).IsNull()) {
			entry.has_extent = true;
			entry.min_x = extent->GetValue(0, 0).GetValue<double>();
			entry.min_y = extent->GetValue(1, 0).GetValue<double>();
			entry.min_z = extent->GetValue(2, 0).GetValue<double>();
			entry.max_x = extent->GetValue(3, 0).GetValue<double>();
			entry.max_y = extent->GetValue(4, 0).GetValue<double>();
			entry.max_z = extent->GetValue(5, 0).GetValue<double>();
		}
		facts.push_back(std::move(entry));
	}
	return facts;
}

json ColumnEntry(const ColumnFacts &facts, const json &crs, bool for_geo) {
	json entry;
	entry["encoding"] = "WKB";
	entry["geometry_types"] = json(facts.geometry_types);
	entry["crs"] = crs;
	entry["edges"] = "planar";
	if (!for_geo) {
		// GeoParquet's planar `orientation` cannot express 3D winding, so CityParquet
		// states it in `city` instead. A writer must always be explicit, including for
		// the common right-handed case.
		entry["orientation_3d"] = "right-handed";
	}
	if (facts.has_extent) {
		entry["bbox"] = json::array({facts.min_x, facts.min_y, facts.min_z, facts.max_x, facts.max_y, facts.max_z});
	}
	return entry;
}

//! The `city` object. Every field the writer does not recompute is carried verbatim from
//! the bookkeeping table -- `source_version` and `other` hold non-derivable provenance,
//! so dropping them would make even an unmodified read/write cycle lossy.
std::string BuildCityJson(const std::string &carried, const json &crs, const std::vector<ColumnFacts> &facts,
                          const std::vector<std::string> &attributes, const std::string &source_format) {
	json city = json::object();
	if (!carried.empty()) {
		try {
			auto parsed = json::parse(carried);
			if (parsed.is_object()) {
				city = std::move(parsed);
			}
		} catch (const std::exception &) {
			// A footer we cannot parse is not a reason to refuse the write; it is
			// regenerated below from what we do know.
		}
	}
	city["version"] = CITYPARQUET_VERSION;
	city["crs"] = crs;
	if (!source_format.empty()) {
		city["source_format"] = source_format;
	}

	if (!facts.empty()) {
		json columns = json::array();
		std::string primary;
		for (const auto &entry : facts) {
			columns.push_back(ColumnEntry(entry, crs, false));
			primary = entry.name; // the finest analysis geometry, i.e. the last LoD column
		}
		city["columns"] = std::move(columns);
		city["primary_column"] = primary;
	} else {
		city.erase("columns");
		city.erase("primary_column");
	}
	city["attributes"] = json(attributes);
	return city.dump();
}

//! The `geo` object, or empty when no column qualifies -- in which case the caller must
//! write no `geo` key at all. GeoParquet requires a non-empty `columns` map and a
//! `primary_column`, so a solid-only table simply has no legal `geo` object: it is a
//! valid CityParquet table that is not a GeoParquet file.
std::string BuildGeoJson(const json &crs, const std::vector<ColumnFacts> &facts) {
	json columns = json::object();
	std::string primary;
	for (const auto &entry : facts) {
		if (!entry.Legal()) {
			continue;
		}
		columns[entry.name] = ColumnEntry(entry, crs, true);
		primary = entry.name;
	}
	if (columns.empty()) {
		return std::string();
	}
	json geo;
	geo["version"] = "1.1.0";
	geo["primary_column"] = primary;
	geo["columns"] = std::move(columns);
	return geo.dump();
}

} // namespace

static unique_ptr<FunctionData> WriteBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<WriteBindData>();
	result->schema = StringValue::Get(input.inputs[0]);
	result->directory = StringValue::Get(input.inputs[1]);
	for (auto &entry : input.named_parameters) {
		if (entry.first == "crs") {
			result->crs = StringValue::Get(entry.second);
		} else if (entry.first == "source_format") {
			result->source_format = StringValue::Get(entry.second);
		}
	}

	names = {"file", "action", "rows", "bytes"};
	return_types = {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR),
	                LogicalType(LogicalTypeId::BIGINT), LogicalType(LogicalTypeId::BIGINT)};
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> WriteInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<WriteBindData>();
	auto state = make_uniq<WriteGlobalState>();

	auto object_tables = ObjectTablesInSchema(context, bind_data.schema);
	auto sidecars = SidecarTablesInSchema(context, bind_data.schema);

	// A separate connection: this function must run queries, and the caller's context is
	// mid-execution and holding its own lock. The consequence is that only COMMITTED
	// state is visible -- mutate, commit, then write.
	Connection connection(DatabaseInstance::GetDatabase(context));
	// A fresh context does not inherit the caller's loaded extensions, and the COPY below
	// needs the parquet writer. Failure is ignored: if parquet is unavailable the COPY
	// reports it directly, which is a clearer error than one raised here.
	connection.Query("LOAD parquet;");

	// The carried footer, per table.
	std::map<std::string, std::string> carried;
	auto bookkeeping = connection.Query("SELECT table_name, city FROM " +
	                                    QualifiedName(bind_data.schema, "__cityparquet"));
	if (!bookkeeping->HasError()) {
		for (idx_t row = 0; row < bookkeeping->RowCount(); row++) {
			auto city = bookkeeping->GetValue(1, row);
			if (!city.IsNull()) {
				carried[bookkeeping->GetValue(0, row).ToString()] = city.ToString();
			}
		}
	}

	// The CRS. A hand-rolled load leaves the footer NULL, so it must be supplied; writing
	// a file that carries CRS-bearing coordinates with no `crs` silently mis-georeferences
	// it, which the specification makes a conversion error rather than an omission.
	std::string crs_source = bind_data.crs;
	if (crs_source.empty()) {
		for (const auto &entry : carried) {
			json parsed;
			try {
				parsed = json::parse(entry.second);
			} catch (const std::exception &) {
				continue;
			}
			auto found = parsed.find("crs");
			if (found != parsed.end() && !found->is_null()) {
				crs_source = found->dump();
				break;
			}
		}
	}
	if (crs_source.empty()) {
		throw InvalidInputException(
		    "cityparquet_write: no crs. The package's footer carries none (a hand-rolled load discards it), so "
		    "it must be given explicitly: cityparquet_write('%s', '%s', crs => 'EPSG:7415'). Writing geometry "
		    "with no CRS would silently mis-georeference the package.",
		    bind_data.schema, bind_data.directory);
	}
	json crs_json = nullptr;
	{
		auto projjson = ProjjsonForReferenceSystem(crs_source);
		if (projjson.has_value()) {
			crs_json = json_utils::ParseJson(projjson.value());
		} else {
			try {
				crs_json = json::parse(crs_source);
			} catch (const std::exception &) {
				throw InvalidInputException("cityparquet_write: cannot resolve CRS '%s' to PROJJSON", crs_source);
			}
		}
	}

	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(bind_data.directory)) {
		fs.CreateDirectory(bind_data.directory);
	}

	PackageInventory inventory;

	auto write_table = [&](const std::string &table, bool is_object) {
		auto count = Run(connection, "SELECT COUNT(*) FROM " + QualifiedName(bind_data.schema, table));
		const auto rows = count->GetValue(0, 0).GetValue<int64_t>();
		if (rows == 0 && is_object) {
			// "No file for a module with no rows."
			return;
		}
		if (table == "materials") {
			inventory.materials = rows;
		} else if (table == "textures") {
			inventory.textures = rows;
		} else if (table == "geometry_templates") {
			// A package whose objects carry only GeometryInstance geometry has no
			// populated geometry_lod* column in any object table -- its LoDs live here.
			// city3d:lods is the union across every file in the package, so leaving the
			// sidecar out would report an empty LoD set for a perfectly valid package.
			CollectTemplateInventory(connection, context, bind_data.schema, table, inventory);
		}

		std::string kv;
		if (is_object) {
			auto facts = CollectFacts(connection, context, bind_data.schema, table);
			std::vector<std::string> attributes;
			auto columns = Run(connection, "SELECT column_name FROM (DESCRIBE SELECT * FROM " +
			                                   QualifiedName(bind_data.schema, table) + ")");
			for (idx_t row = 0; row < columns->RowCount(); row++) {
				const auto name = columns->GetValue(0, row).ToString();
				if (!IsReservedColumnName(name)) {
					attributes.push_back(name);
				}
			}
			CollectInventory(connection, bind_data.schema, table, rows, facts, attributes, inventory);
			auto carried_entry = carried.find(table);
			const auto city = BuildCityJson(carried_entry == carried.end() ? std::string() : carried_entry->second,
			                                crs_json, facts, attributes, bind_data.source_format);
			const auto geo = BuildGeoJson(crs_json, facts);
			kv = "city: " + Literal(city);
			if (!geo.empty()) {
				// Only when a column actually qualifies. See BuildGeoJson.
				kv += ", geo: " + Literal(geo);
			}
		} else {
			json city = json::object();
			city["version"] = CITYPARQUET_VERSION;
			kv = "city: " + Literal(city.dump());
		}

		const auto file = table + ".parquet";
		const auto path = fs.JoinPath(bind_data.directory, file);
		Run(connection, "COPY " + QualifiedName(bind_data.schema, table) + " TO " + Literal(path) +
		                    " (FORMAT PARQUET, KV_METADATA {" + kv + "});");

		WrittenFile written;
		written.file = file;
		written.action = "written";
		written.rows = rows;
		if (fs.FileExists(path)) {
			auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);
			written.bytes = static_cast<int64_t>(fs.GetFileSize(*handle));
		}
		state->files.push_back(std::move(written));
	};

	for (const auto &table : object_tables) {
		write_table(table, true);
	}
	for (const auto &sidecar : sidecars) {
		write_table(sidecar, false);
	}

	// metadata.json -- the package's STAC Item. Written last: its asset inventory and
	// sizes depend on the files above having reached their final bytes.
	{
		json item;
		item["type"] = "Feature";
		item["stac_version"] = "1.0.0";
		item["stac_extensions"] = json::array(
		    {"https://raw.githubusercontent.com/cityjson/stac-city3d/main/json-schema/schema.json",
		     "https://stac-extensions.github.io/projection/v1.1.0/schema.json",
		     "https://stac-extensions.github.io/file/v2.1.0/schema.json",
		     "https://stac-extensions.github.io/table/v1.2.0/schema.json"});
		item["id"] = bind_data.schema;
		item["links"] = json::array();

		// STAC defines the Item's own `geometry` and `bbox` in WGS84, and a CityParquet
		// package's coordinates are in its own projected CRS. Reprojecting needs a proj
		// library this extension does not carry, and putting projected numbers in a field
		// documented as WGS84 would be worse than omitting them -- a consumer filtering
		// spatially would silently place the package somewhere off the coast of Africa.
		// So `geometry` is null, `bbox` is omitted (STAC requires it only alongside a
		// non-null geometry), and `proj:bbox` below carries the real extent, which is
		// exactly what the Projection extension exists for.
		item["geometry"] = nullptr;

		json properties = json::object();
		properties["city3d:version"] = CITYPARQUET_VERSION;
		// Every one of these is a union or a sum across the package's files. The footer
		// answers only for the file it lives in.
		properties["city3d:lods"] = json(inventory.lods);
		properties["city3d:co_types"] = json(inventory.co_types);
		properties["city3d:city_objects"] = inventory.city_objects;
		properties["city3d:attributes"] = json(inventory.attributes);
		properties["city3d:semantic_surfaces"] = inventory.semantic_surfaces;
		properties["city3d:materials"] = inventory.materials > 0;
		properties["city3d:textures"] = inventory.textures > 0;
		// Projection extension: the CRS every geometry and bbox in the package shares.
		properties["proj:projjson"] = crs_json;
		if (inventory.has_extent) {
			properties["proj:bbox"] = json::array({inventory.min_x, inventory.min_y, inventory.min_z, inventory.max_x,
			                                       inventory.max_y, inventory.max_z});
		}
		item["properties"] = std::move(properties);

		json assets = json::object();
		for (const auto &written : state->files) {
			json asset;
			asset["href"] = written.file;
			asset["type"] = "application/vnd.apache.parquet";
			asset["file:size"] = written.bytes;
			// Row counts per asset. The specification asks for "the Statistics
			// extension"; for a tabular asset the extension that actually defines a row
			// count is Table, so that is what is declared -- see
			// docs/CITYPARQUET_SPEC_QUESTIONS.md. A sidecar's rows are definitions, not
			// city objects, so they are reported per file rather than folded into
			// city3d:city_objects.
			asset["table:row_count"] = written.rows;
			assets[written.file] = std::move(asset);
		}
		item["assets"] = std::move(assets);

		const auto path = fs.JoinPath(bind_data.directory, "metadata.json");
		const auto text = item.dump(2);
		auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);
		fs.Write(*handle, const_cast<char *>(text.c_str()), static_cast<int64_t>(text.size()));
		handle->Close();

		WrittenFile written;
		written.file = "metadata.json";
		written.action = "written";
		written.rows = 0;
		written.bytes = static_cast<int64_t>(text.size());
		state->files.push_back(std::move(written));
	}

	return std::move(state);
}

static void WriteScan(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<WriteGlobalState>();
	idx_t emitted = 0;
	while (state.offset < state.files.size() && emitted < STANDARD_VECTOR_SIZE) {
		const auto &written = state.files[state.offset];
		output.SetValue(0, emitted, Value(written.file));
		output.SetValue(1, emitted, Value(written.action));
		output.SetValue(2, emitted, Value::BIGINT(written.rows));
		output.SetValue(3, emitted, Value::BIGINT(written.bytes));
		state.offset++;
		emitted++;
	}
	output.SetCardinality(emitted);
}

void RegisterCityParquetWriteFunction(ExtensionLoader &loader) {
	TableFunction func("cityparquet_write",
	                   {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)}, WriteScan,
	                   WriteBind);
	func.init_global = WriteInitGlobal;
	func.named_parameters["crs"] = LogicalType(LogicalTypeId::VARCHAR);
	func.named_parameters["source_format"] = LogicalType(LogicalTypeId::VARCHAR);
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

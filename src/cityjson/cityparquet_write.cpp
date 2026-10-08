#include "cityjson/cityparquet_write.hpp"

#include "cityjson/function_docs.hpp"
#include "cityjson/cityparquet_extensions.hpp"
#include "cityjson/cityparquet_package.hpp"
#include "cityjson/cityparquet_sql_common.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/crs_projjson.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/wkb_encoder.hpp"
#include "cityjson/wkb_extent.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <array>
#include <regex>
#include <cmath>
#include <set>

namespace duckdb {
namespace cityjson {

namespace {

//! WKB type names GeoParquet 1.1 permits (codes 1001-1007, GeometryCollection Z of
//! PolyhedralSurface excluded -- spec 05-metadata.mdx "The geo object"). A column
//! carrying anything else -- any solid-family geometry -- MUST NOT be declared in
//! `geo`: a strict reader that eagerly decodes every declared column rejects the
//! WHOLE file on one it cannot parse, taking a perfectly good footprint column down
//! with it. Matches GeoParquetTypeName in geoparquet_table_function.cpp.
bool GeoParquetLegal(const std::string &type_name) {
	static const std::set<std::string> legal = {"Point Z",      "LineString Z",      "Polygon Z",
	                                            "MultiPoint Z", "MultiLineString Z", "MultiPolygon Z"};
	return legal.count(type_name) > 0;
}

struct ColumnFacts {
	std::string name;
	//! Physical encoding of the column: always "WKB" (spec 05-metadata.mdx;
	//! token vocabulary shared with cityparquet-rs GeometryEncoding).
	std::string encoding = "WKB";
	std::set<std::string> geometry_types;
	bool has_extent = false;
	double min_x = 0, min_y = 0, min_z = 0, max_x = 0, max_y = 0, max_z = 0;

	bool Legal() const {
		if (encoding != "WKB" || geometry_types.empty()) {
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
	//! The catalog the caller's search path resolved `schema` in. The queries
	//! below run on a connection of our own, which never saw that search path,
	//! so every generated name must say the catalog outright.
	std::string catalog;
	std::string directory;
	std::string crs;
	std::string source_format;
	//! Write Parquet bloom filters on the object tables (`bloom => false` writes none).
	bool bloom = true;
	//! Order each object table's rows along a Hilbert curve (`ordering => 'hilbert'`, the
	//! default), or keep the table's own order (`ordering => 'source'`).
	bool hilbert = true;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<WriteBindData>();
		result->schema = schema;
		result->catalog = catalog;
		result->directory = directory;
		result->crs = crs;
		result->source_format = source_format;
		result->bloom = bloom;
		result->hilbert = hilbert;
		return std::move(result);
	}
	bool Equals(const FunctionData &other) const override {
		auto &o = other.Cast<WriteBindData>();
		return catalog == o.catalog && schema == o.schema && directory == o.directory && bloom == o.bloom &&
		       hilbert == o.hilbert;
	}
};

struct WrittenFile {
	std::string file;
	std::string action;
	int64_t rows = 0;
	int64_t bytes = 0;
	//! Object table (a CityGML module) rather than an appearance/implicit-geometry sidecar.
	//! Decides the asset's STAC roles in metadata.json, which is how a reader tells
	//! the two apart without parsing filenames.
	bool is_object = false;
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

//! DuckDB type of one column. Non-templated GetEntry: the templated form
//! ODR-uses TableCatalogEntry::Name (see cityparquet_package.cpp).
LogicalType ColumnDuckType(ClientContext &context, const std::string &schema, const std::string &table,
                           const std::string &column) {
	auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, INVALID_CATALOG, schema, table);
	for (auto &existing : entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		if (StringUtil::Lower(existing.Name()) == StringUtil::Lower(column)) {
			return existing.Type();
		}
	}
	return LogicalType(LogicalTypeId::INVALID);
}

std::string ColumnExpression(const ColumnDefinition &column, const std::set<std::string> &legal_geometry,
                             const std::string &crs);

//! A COPY source list that converts any DuckDB-native GEOMETRY column back to WKB
//! BLOB via ST_AsWKB. enable_geoparquet_conversion (on by default whenever the
//! `parquet` extension reads a footer's `geo` key -- not gated on `spatial` being
//! installed or loaded at all) promotes such a column to GEOMETRY on read; passing
//! that straight through a `COPY table TO ... (FORMAT PARQUET)` lets DuckDB's own
//! GeoParquet writer hook stamp a second `geo` key into the KV metadata alongside
//! this extension's. Parquet permits duplicate keys silently, so a reader taking
//! the wrong one gets a footer this writer never intended. ST_AsWKB rather than a
//! `::BLOB` cast: the cast is registered by `spatial` only once it is loaded,
//! whereas ST_AsWKB/ST_AsBinary ship in DuckDB core (functions.json) and need no
//! extension at all -- so this never requires `spatial` to be installed, and never
//! loads it, so it never re-arms this same collision one layer out for a caller's
//! own COPY over the resulting table.
//! `legal_geometry` names the columns this file declares in `geo`. Those, and
//! only those, are handed to the COPY as DuckDB `GEOMETRY` so the Parquet writer
//! annotates them with the `GEOMETRY` logical type; every other column goes out
//! as a plain `BLOB`.
//!
//! The asymmetry is the spec's declaration rule, not a quirk of this writer: a
//! solid column's `PolyhedralSurface Z` is outside DuckDB's geometry model, so
//! `ST_GeomFromWKB` would reject it here, and a reader meeting the annotation
//! would reject it there.
//!
//! The JSON columns -- `other` (object tables and the materials / textures sidecars)
//! and the `surfaces` field of every geometry-properties struct -- go out through
//! cityparquet_json, whose result carries DuckDB's JSON type, which the Parquet
//! writer annotates with the JSON logical type (spec 02-object-table-schema.mdx). The
//! tables hold them as text, or as JSON when loaded from a file that declared it, so
//! both are cast to text first.
//!
//! An object table goes out in the specification's column order (02-object-table-schema
//! .mdx: reserved columns first, in their order, per-LoD groups by LoD, attributes
//! last), whatever order the table's columns were added in, and with only the LoD
//! groups some row populates (`populated_lods`, as `lod2_2` suffixes) -- "a table carries
//! exactly the LoD columns its data needs", and a geometry column every row leaves NULL
//! would have no geometry type to declare in `city.columns`. Sidecars pass nullptr and
//! keep their columns as they are.
std::string CopySourceList(ClientContext &context, const std::string &schema, const std::string &table,
                           const std::set<std::string> &legal_geometry, const std::string &crs,
                           const std::set<std::string> *populated_lods) {
	auto &entry = Catalog::GetEntry(context, CatalogType::TABLE_ENTRY, INVALID_CATALOG, schema, table);
	static const std::vector<std::string> head = {"id",       "feature_id",     "object_type", "parents",
	                                              "children", "children_roles", "address",     "bbox"};
	static const std::vector<std::string> group = {"geometry_lod", "geometry_properties_lod", "material_lod",
	                                               "texture_lod"};
	// (rank, LoD major, LoD minor, kind, table position): stable whatever the input.
	std::vector<std::pair<std::array<int64_t, 5>, std::string>> ranked;
	int64_t position = 0;
	for (auto &column : entry.Cast<TableCatalogEntry>().GetColumns().Logical()) {
		const auto lowered = StringUtil::Lower(column.Name());
		std::array<int64_t, 5> rank {3, 0, 0, 0, position++};
		const auto in_head = std::find(head.begin(), head.end(), lowered);
		if (populated_lods != nullptr && in_head != head.end()) {
			rank[0] = 0;
			rank[1] = in_head - head.begin();
		} else if (populated_lods != nullptr && (lowered == "implicit_geometry" || lowered == "other")) {
			rank[0] = 2;
			rank[1] = lowered == "other" ? 1 : 0;
		} else if (populated_lods != nullptr) {
			for (size_t kind = 0; kind < group.size(); kind++) {
				if (!MatchesLodSuffix(lowered, group[kind])) {
					continue;
				}
				const auto suffix = lowered.substr(group[kind].size() - 3); // "lod2_2"
				if (populated_lods->count(suffix) == 0) {
					rank[0] = -1; // an LoD no row populates: not written
					break;
				}
				const auto lod = LODTableUtils::ParseLODFromSuffix(suffix);
				const auto dot = lod.find('.');
				rank[0] = 1;
				rank[1] = std::stoll(lod.substr(0, dot));
				rank[2] = dot == std::string::npos ? 0 : std::stoll(lod.substr(dot + 1));
				rank[3] = static_cast<int64_t>(kind);
				break;
			}
		}
		if (rank[0] < 0) {
			continue;
		}
		ranked.emplace_back(rank, ColumnExpression(column, legal_geometry, crs));
	}
	std::stable_sort(ranked.begin(), ranked.end(), [](const auto &a, const auto &b) { return a.first < b.first; });
	vector<string> parts;
	for (auto &entry_part : ranked) {
		parts.push_back(std::move(entry_part.second));
	}
	return StringUtil::Join(parts, ", ");
}

//! One column of CopySourceList: the JSON columns typed JSON, a declared geometry
//! column GEOMETRY, every other geometry WKB.
std::string ColumnExpression(const ColumnDefinition &column, const std::set<std::string> &legal_geometry,
                             const std::string &crs) {
	{
		const auto quoted = KeywordHelper::WriteOptionallyQuoted(column.Name());
		if (StringUtil::Lower(column.Name()) == "other") {
			return "cityparquet_json(CAST(" + quoted + " AS VARCHAR)) AS " + quoted;
		}
		if (HasSurfacesField(column.Type())) {
			// Rebuilt field by field so only `surfaces` changes type; the CASE keeps a
			// NULL struct NULL, which struct_pack alone would turn into a struct of NULLs.
			vector<string> fields;
			for (const auto &child : StructType::GetChildTypes(column.Type())) {
				const auto field = KeywordHelper::WriteOptionallyQuoted(child.first);
				const auto ref = quoted + "." + field;
				fields.push_back(field + " := " +
				                 (child.first == "surfaces" ? "cityparquet_json(CAST(" + ref + " AS VARCHAR))" : ref));
			}
			return "CASE WHEN " + quoted + " IS NULL THEN NULL ELSE struct_pack(" + StringUtil::Join(fields, ", ") +
			       ") END AS " + quoted;
		}
		const auto is_geometry = column.Type().id() == LogicalTypeId::GEOMETRY;
		if (legal_geometry.count(column.Name()) > 0) {
			// Annotated: keep it GEOMETRY-typed through the COPY, promoting the
			// WKB blob when the source table holds one.
			auto expr = is_geometry ? quoted : "ST_GeomFromWKB(" + quoted + ")";
			// The logical type's own `crs` parameter, carrying the same PROJJSON
			// `geo` states. Leaving it unset is not neutral: the
			// Parquet spec reads an absent `crs` as OGC:CRS84, so a package in
			// RD New would announce itself as lon/lat degrees to any reader
			// that trusts the annotation over the footer.
			if (!crs.empty()) {
				expr = "ST_SetCRS(" + expr + ", " + Literal(crs) + ")";
			}
			return expr + " AS " + quoted;
		}
		const auto ref = GeometryColumnRef(quoted, column.Type());
		return is_geometry ? (ref + " AS " + quoted) : ref;
	}
}

//! Run a query on the internal connection, throwing its error rather than swallowing it.
unique_ptr<MaterializedQueryResult> Run(Connection &connection, const std::string &sql) {
	auto result = connection.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("cityparquet_write: %s\n(while running: %s)", result->GetError(), sql);
	}
	return result;
}

//! Refuses a table holding a name with CityJSON's extension marker `+`. A package
//! stores every extension name with its namespace prefix (spec 06-extensions.mdx), which
//! insert_cityjson applies; a hand-rolled load keeps the reader's CityJSON names.
void RefusePlusNames(Connection &connection, ClientContext &context, const std::string &catalog,
                     const std::string &schema, const std::string &table, bool is_object) {
	const auto refuse = [&](const std::string &what) {
		throw InvalidInputException("cityparquet_write: %s of '%s.%s' carries CityJSON's extension marker `+`; a "
		                            "CityParquet package stores an extension name with its extension's namespace "
		                            "prefix, declared in city.extensions -- add CityJSON to a package with "
		                            "insert_cityjson, which applies it",
		                            what, schema, table);
	};
	for (const auto &column : TableColumns(context, schema, table)) {
		if (IsPlusName(column.name)) {
			refuse("the column '" + column.name + "'");
		}
		const bool is_properties = MatchesLodSuffix(StringUtil::Lower(column.name), "geometry_properties_lod");
		if (is_properties && HasSurfacesField(column.type)) {
			// Cast: `surfaces` is JSON in a table loaded from a file that declares it, and
			// regexp_* take text.
			const auto surfaces = "CAST(" + KeywordHelper::WriteOptionallyQuoted(column.name) + ".surfaces AS VARCHAR)";
			auto found = Run(connection, "SELECT regexp_extract(" + surfaces + ", " +
			                                 Literal(R"re("type"\s*:\s*"(\+[^"]*)")re") + ", 1) FROM " +
			                                 QualifiedName(catalog, schema, table) + " WHERE regexp_matches(" +
			                                 surfaces + ", " + Literal(R"re("type"\s*:\s*"\+)re") + ") LIMIT 1");
			if (found->RowCount() > 0) {
				refuse("the semantic surface type '" + found->GetValue(0, 0).ToString() + "' in '" + column.name + "'");
			}
		}
	}
	if (is_object) {
		auto found = Run(connection, "SELECT object_type FROM " + QualifiedName(catalog, schema, table) +
		                                 " WHERE starts_with(object_type, '+') LIMIT 1");
		if (found->RowCount() > 0) {
			refuse("the object type '" + found->GetValue(0, 0).ToString() + "'");
		}
	}
}

//! A CityJSON referenceDate as STAC's RFC 3339 `datetime`, as the reference writer
//! takes it: a full RFC 3339 timestamp as it stands, a bare date as midnight UTC, and
//! anything else -- an impossible date, a timestamp without an offset -- as nothing.
std::string StacDatetime(const std::string &text) {
	static const std::regex rfc3339(R"(^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$)");
	static const std::regex day(R"(^\d{4}-\d{2}-\d{2}$)");
	const bool full = std::regex_match(text, rfc3339);
	if (!full && !std::regex_match(text, day)) {
		return std::string();
	}
	date_t parsed;
	idx_t pos = 0;
	bool special = false;
	if (Date::TryConvertDate(text.c_str(), 10, pos, parsed, special, true) != DateCastResult::SUCCESS) {
		return std::string();
	}
	if (full) {
		const auto hour = std::stoi(text.substr(11, 2));
		const auto minute = std::stoi(text.substr(14, 2));
		const auto second = std::stoi(text.substr(17, 2));
		if (hour > 23 || minute > 59 || second > 60) {
			return std::string();
		}
		return text;
	}
	return text + "T00:00:00Z";
}

//! Refuses an object table whose `other` holds JSON that is not an object (spec
//! 02-object-table-schema.mdx, "The `other` column": a reader restores every entry
//! into the object's attributes, and MUST reject a cell that is not an object). Text
//! that is not JSON at all is refused by cityparquet_json as the file is written.
void RefuseNonObjectOther(Connection &connection, ClientContext &context, const std::string &catalog,
                          const std::string &schema, const std::string &table) {
	if (!HasColumn(context, schema, table, "other")) {
		return;
	}
	auto found =
	    Run(connection, "SELECT id FROM " + QualifiedName(catalog, schema, table) +
	                        " WHERE other IS NOT NULL AND NOT starts_with(ltrim(CAST(other AS VARCHAR), ' \t\n\r'), "
	                        "'{') LIMIT 1");
	if (found->RowCount() > 0) {
		throw InvalidInputException("cityparquet_write: object '%s' of '%s.%s' has an `other` that is not a JSON "
		                            "object; its members are restored as attributes, so it must be one (spec "
		                            "02-object-table-schema.mdx)",
		                            found->GetValue(0, 0).ToString(), schema, table);
	}
}

//! Fold one object table into the package-level inventory.
void CollectInventory(Connection &connection, const std::string &catalog, const std::string &schema,
                      const std::string &table, int64_t rows, const std::vector<ColumnFacts> &facts,
                      const std::vector<std::string> &attributes, const std::vector<ExtensionDeclaration> &declarations,
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
	// An extension class is listed with CityJSON's `+`, not its namespace prefix.
	auto types = Run(connection, "SELECT DISTINCT object_type FROM " + QualifiedName(catalog, schema, table) +
	                                 " WHERE object_type IS NOT NULL");
	for (idx_t row = 0; row < types->RowCount(); row++) {
		inventory.co_types.insert(RestorePlusName(types->GetValue(0, row).ToString(), declarations));
	}

	if (inventory.semantic_surfaces) {
		return;
	}
	for (const auto &column : facts) {
		const auto properties = "geometry_properties_" + column.name.substr(std::string("geometry_").size());
		auto present = Run(connection, "SELECT COUNT(*) FROM " + QualifiedName(catalog, schema, table) + " WHERE " +
		                                   KeywordHelper::WriteOptionallyQuoted(properties) + ".surfaces IS NOT NULL");
		if (present->GetValue(0, 0).GetValue<int64_t>() > 0) {
			inventory.semantic_surfaces = true;
			return;
		}
	}
}

//! The implicit_geometries sidecar's contribution to the package inventory: its LoDs and
//! whether any relative geometry carries semantic surfaces. Relative geometries are
//! stored in local, unplaced coordinates, so they contribute no extent.
void CollectImplicitGeometryInventory(Connection &connection, ClientContext &context, const std::string &catalog,
                                      const std::string &schema, const std::string &table,
                                      PackageInventory &inventory) {
	for (const auto &column : GeometryLodColumns(context, schema, table)) {
		const auto suffix = column.substr(std::string("geometry_").size());
		auto lod = LODTableUtils::ParseLODFromSuffix(suffix);
		if (!lod.empty()) {
			inventory.lods.insert(lod);
		}
		if (inventory.semantic_surfaces) {
			continue;
		}
		auto present = Run(connection, "SELECT COUNT(*) FROM " + QualifiedName(catalog, schema, table) + " WHERE " +
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
std::vector<ColumnFacts> CollectFacts(Connection &connection, ClientContext &context, const std::string &catalog,
                                      const std::string &schema, const std::string &table) {
	std::vector<ColumnFacts> facts;
	for (const auto &column : GeometryLodColumns(context, schema, table)) {
		ColumnFacts entry;
		entry.name = column;
		const auto quoted = KeywordHelper::WriteOptionallyQuoted(column);
		auto col_type = ColumnDuckType(context, schema, table, column);
		// GeometryColumnRef: the column arrives here already decoded to DuckDB's
		// native GEOMETRY type when it was promoted -- declared in the file's
		// GeoParquet `geo` footer (enable_geoparquet_conversion, on by default,
		// gated only on the `parquet` extension, not on `spatial`). The underlying
		// bytes are the same WKB either way.
		const auto wkb_expr = GeometryColumnRef(quoted, col_type);
		auto result = Run(connection, "SELECT DISTINCT cityjson_wkb_geometry_type(" + wkb_expr + ") FROM " +
		                                  QualifiedName(catalog, schema, table) + " WHERE " + quoted + " IS NOT NULL");
		for (idx_t row = 0; row < result->RowCount(); row++) {
			auto value = result->GetValue(0, row);
			if (!value.IsNull()) {
				entry.geometry_types.insert(value.ToString());
			}
		}
		if (entry.geometry_types.empty()) {
			continue; // the column exists but no row populates it
		}
		auto extent = Run(connection, "SELECT min(e.xmin), min(e.ymin), min(e.zmin), max(e.xmax), max(e.ymax), "
		                              "max(e.zmax) FROM (SELECT cityjson_wkb_extent(" +
		                                  wkb_expr + ") AS e FROM " + QualifiedName(catalog, schema, table) +
		                                  " WHERE " + quoted + " IS NOT NULL) t");
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
	entry["encoding"] = facts.encoding;
	entry["geometry_types"] = json(facts.geometry_types);
	entry["crs"] = crs;
	entry["edges"] = "planar";
	if (!for_geo) {
		// city.columns is a LIST of entries, so each one must carry its column's
		// name (spec 05-metadata.mdx; required by cityparquet-rs CityColumnEntry).
		// geo.columns is a MAP keyed by name and takes no name field.
		entry["name"] = facts.name;
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
	// Always written, never omitted: an object table holds CRS-bearing coordinates, so the
	// key is either PROJJSON or an explicit null, and each column entry mirrors it.
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

//! Row-group size of every package COPY -- DuckDB's own default, stated so the
//! object tables' dictionary cut-off below can equal it.
constexpr idx_t PACKAGE_ROW_GROUP_SIZE = 122880;
//! Cap on one string dictionary page. It restricts the chunks DuckDB would
//! otherwise dictionary-encode: across a full row group it is roughly 68 bytes
//! per row for a near-unique column, so identifier dictionaries stay far below
//! it, a wide near-unique string exceeds it and falls back to PLAIN, and a
//! compact WKB or JSON column can fit under it. It is a further restriction,
//! not the whole rule -- a type DuckDB never dictionary-encodes, Boolean among
//! them, is PLAIN however small its vocabulary.
constexpr idx_t STRING_DICTIONARY_PAGE_LIMIT = 8388608;

//! The COPY options that decide bloom filters (spec 02-object-table-schema.mdx,
//! "Bloom filters"). DuckDB writes a filter only for a dictionary-encoded chunk,
//! and its dictionary and bloom options are file-wide, so an object table raises
//! the dictionary cut-off to the row-group size, so the chunks it does
//! dictionary-encode -- `id` and `feature_id` among them -- carry a filter.
//!
//! Eligibility is therefore CONDITIONAL on the chunk actually being
//! dictionary-encoded, not a fixed column list. That excludes a type whose
//! writer never uses a dictionary, Boolean among them: PLAIN and unfiltered
//! however small its vocabulary. The dictionary-page cap restricts the rest: a
//! high-cardinality string averaging more than about 68 bytes over a full
//! 122 880-row group exceeds it, is written PLAIN and carries no filter; a
//! compact WKB or JSON column can stay under it and carry one, in any row
//! group and not only a short trailing one. On the packages measured
//! so far the filtered set is a superset of the reference writer's -- on delft
//! (2 231 rows, one row group) 68 of 115 column chunks carry a filter,
//! including numeric and temporal attributes, the `bbox` leaves, list elements
//! and the geometry columns -- but that is an observation on those data, not a
//! property of the option set. Documented in 06-resources/02-software.mdx.
//! Sidecars carry none.
std::string BloomCopyOptions(bool is_object, bool bloom) {
	const auto row_groups = ", ROW_GROUP_SIZE " + std::to_string(PACKAGE_ROW_GROUP_SIZE);
	if (!is_object || !bloom) {
		return row_groups + ", WRITE_BLOOM_FILTER false";
	}
	return row_groups + ", DICTIONARY_SIZE_LIMIT " + std::to_string(PACKAGE_ROW_GROUP_SIZE) +
	       ", STRING_DICTIONARY_PAGE_SIZE_LIMIT " + std::to_string(STRING_DICTIONARY_PAGE_LIMIT) +
	       ", WRITE_BLOOM_FILTER true, BLOOM_FILTER_FALSE_POSITIVE_RATIO 0.01";
}

//! The internal connection's temp table of per-feature Hilbert keys.
constexpr const char *HILBERT_KEYS = "temp.__cityparquet_hilbert_keys";

//! Fill HILBERT_KEYS with one key per feature that has geometry, the reference writer's
//! (cityparquet-rs, `order.rs`): the curve is laid over the x/y extent of every geometry
//! in every object table; a feature's point on it is the centre of the x/y extent of
//! every coordinate its rows carry, across every object table -- its geometry at every
//! LoD, and also its implicit geometries' reference points and its addresses'
//! locations, which are in a feature's vertex pool but are not geometry the curve is
//! laid over. Extents come from the WKB, not from `bbox`, which also unions a source's
//! declared extents. False when the package holds no geometry, so there is nothing to
//! order by.
bool WriteHilbertKeys(Connection &connection, ClientContext &context, const std::string &catalog,
                      const std::string &schema, const std::vector<std::string> &tables) {
	std::vector<std::string> extents;
	std::vector<std::string> placed;
	for (const auto &table : tables) {
		if (!HasColumn(context, schema, table, "feature_id")) {
			continue;
		}
		const auto qualified = QualifiedName(catalog, schema, table);
		for (const auto &column : GeometryLodColumns(context, schema, table)) {
			const auto quoted = KeywordHelper::WriteOptionallyQuoted(column);
			extents.push_back("SELECT feature_id, cityjson_wkb_extent(" +
			                  GeometryColumnRef(quoted, ColumnDuckType(context, schema, table, column)) +
			                  ") AS e FROM " + qualified + " WHERE " + quoted + " IS NOT NULL");
		}
		if (HasColumn(context, schema, table, "implicit_geometry")) {
			placed.push_back("SELECT feature_id, cityjson_wkb_extent(implicit_geometry.point) AS e FROM " + qualified +
			                 " WHERE implicit_geometry.point IS NOT NULL");
		}
		if (HasColumn(context, schema, table, "address")) {
			placed.push_back("SELECT feature_id, cityjson_wkb_extent(a.location) AS e FROM (SELECT feature_id, "
			                 "unnest(address) AS a FROM " +
			                 qualified + ") WHERE a.location IS NOT NULL");
		}
	}
	if (extents.empty()) {
		return false;
	}
	auto everything = extents;
	everything.insert(everything.end(), placed.begin(), placed.end());
	Run(connection, "CREATE OR REPLACE TEMP TABLE " + std::string(HILBERT_KEYS) + " AS\nWITH extents AS (" +
	                    Join(extents, "\n  UNION ALL ") + "),\ncoordinates AS (" + Join(everything, "\n  UNION ALL ") +
	                    "),\n"
	                    "features AS (SELECT feature_id, (min(e.xmin) + max(e.xmax)) / 2 AS x, "
	                    "(min(e.ymin) + max(e.ymax)) / 2 AS y FROM coordinates GROUP BY feature_id),\n"
	                    "dataset AS (SELECT min(e.xmin) AS xmin, min(e.ymin) AS ymin, max(e.xmax) AS xmax, "
	                    "max(e.ymax) AS ymax FROM extents)\n"
	                    "SELECT feature_id, cityparquet_hilbert(x, y, xmin, ymin, xmax, ymax) AS hilbert_key "
	                    "FROM features, dataset;");
	return true;
}

} // namespace

//! `v`'s cell along one axis of 2^16, clamped into the extent. order.rs `normalise_axis`.
static uint32_t HilbertCell(double v, double min, double max) {
	constexpr uint32_t SIDE = 1U << 16U;
	const double span = max - min;
	// `!(span > 0)` rather than `span <= 0`: a NaN span lands here too, as it lands on
	// cell 0 in the reference writer (Rust's saturating float-to-int cast of NaN).
	if (!(span > 0)) {
		return 0;
	}
	double t = (v - min) / span;
	if (!(t > 0)) {
		t = 0; // also NaN
	} else if (t > 1) {
		t = 1;
	}
	// std::round, like Rust's f64::round, rounds half away from zero.
	const double cell = std::round(t * (static_cast<double>(SIDE) - 1.0));
	return std::min(static_cast<uint32_t>(cell), SIDE - 1);
}

uint32_t HilbertIndex(double x, double y, double xmin, double ymin, double xmax, double ymax) {
	constexpr uint32_t SIDE = 1U << 16U;
	uint32_t cx = HilbertCell(x, xmin, xmax);
	uint32_t cy = HilbertCell(y, ymin, ymax);
	// The classic xy2d (order.rs `xy2d` / `rotate`): each step takes one bit off both
	// coordinates, the coarsest first, so the first quadrant choice dominates the index.
	// The rotation reflects against the whole side, not the shrinking sub-square.
	uint32_t d = 0;
	for (uint32_t s = SIDE / 2; s > 0; s /= 2) {
		const uint32_t rx = (cx & s) > 0 ? 1 : 0;
		const uint32_t ry = (cy & s) > 0 ? 1 : 0;
		d += s * s * ((3 * rx) ^ ry);
		if (ry == 0) {
			if (rx == 1) {
				cx = SIDE - 1 - cx;
				cy = SIDE - 1 - cy;
			}
			std::swap(cx, cy);
		}
	}
	return d;
}

//! cityparquet_hilbert(x, y, xmin, ymin, xmax, ymax): NULL when any argument is.
static void HilbertFunction(DataChunk &args, ExpressionState &, Vector &result) {
	constexpr idx_t ARGUMENTS = 6;
	std::array<UnifiedVectorFormat, ARGUMENTS> formats;
	for (idx_t i = 0; i < ARGUMENTS; i++) {
		args.data[i].ToUnifiedFormat(args.size(), formats[i]);
	}
	result.SetVectorType(::duckdb::VectorType::FLAT_VECTOR);
	auto *out = FlatVector::GetData<uint32_t>(result);
	auto &validity = FlatVector::Validity(result);
	for (idx_t row = 0; row < args.size(); row++) {
		std::array<double, ARGUMENTS> values {};
		bool valid = true;
		for (idx_t i = 0; i < ARGUMENTS; i++) {
			const auto index = formats[i].sel->get_index(row);
			if (!formats[i].validity.RowIsValid(index)) {
				valid = false;
				break;
			}
			values[i] = UnifiedVectorFormat::GetData<double>(formats[i])[index];
		}
		if (!valid) {
			validity.SetInvalid(row);
			continue;
		}
		out[row] = HilbertIndex(values[0], values[1], values[2], values[3], values[4], values[5]);
	}
	if (args.AllConstant()) {
		result.SetVectorType(::duckdb::VectorType::CONSTANT_VECTOR);
	}
}

static unique_ptr<FunctionData> WriteBind(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<WriteBindData>();
	result->schema = StringValue::Get(input.inputs[0]);
	// Resolve through the caller's context, exactly as the column helpers do, then keep
	// the catalog it landed in.
	auto &schema_entry = Catalog::GetSchema(context, INVALID_CATALOG, result->schema);
	result->catalog = schema_entry.catalog.GetName();
	result->directory = StringValue::Get(input.inputs[1]);
	for (auto &entry : input.named_parameters) {
		if (entry.first == "crs") {
			result->crs = StringValue::Get(entry.second);
		} else if (entry.first == "source_format") {
			result->source_format = StringValue::Get(entry.second);
		} else if (entry.first == "bloom") {
			result->bloom = BooleanValue::Get(entry.second);
		} else if (entry.first == "ordering") {
			const auto ordering = StringUtil::Lower(StringValue::Get(entry.second));
			if (ordering != "hilbert" && ordering != "source") {
				throw BinderException("cityparquet_write: ordering must be 'hilbert' or 'source', got '%s'",
				                      StringValue::Get(entry.second));
			}
			result->hilbert = ordering == "hilbert";
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
	                                    QualifiedName(bind_data.catalog, bind_data.schema, "__cityparquet"));
	if (!bookkeeping->HasError()) {
		for (idx_t row = 0; row < bookkeeping->RowCount(); row++) {
			auto city = bookkeeping->GetValue(1, row);
			if (!city.IsNull()) {
				carried[bookkeeping->GetValue(0, row).ToString()] = city.ToString();
			}
		}
	}

	// The CRS, tri-state exactly as GeoParquet (spec 05-metadata.mdx, "CRS rules"): a
	// PROJJSON object when it is known, an explicit `null` when the package holds
	// CRS-bearing coordinates whose CRS is unknown or unresolvable, and the key absent
	// only for a file holding no CRS-bearing coordinate at all (the sidecars below).
	//
	// A hand-rolled load leaves the footer NULL, so `crs =>` is how the CRS reaches this
	// function then. When nothing supplies one, the write proceeds with an explicit null
	// and a warning rather than failing: refusing the write does not make the CRS
	// knowable, and the specification amends the earlier hard-error rule. Omitting the
	// key is the one thing that would be wrong -- an absent `crs` asserts OGC:CRS84, and
	// a package in RD New would then read as lon/lat degrees.
	std::string crs_source = bind_data.crs;
	// An explicitly supplied CRS is the user's own assertion; only a source-derived one
	// falls back to null, so a typo in `crs =>` is still reported rather than nulled.
	const bool crs_from_parameter = !crs_source.empty();
	if (crs_source.empty()) {
		for (const auto &entry : carried) {
			json parsed;
			try {
				parsed = json::parse(entry.second);
			} catch (const std::exception &) {
				continue;
			}
			auto found = parsed.find("crs");
			// An explicit null in a carried footer is a KNOWN unknown, not a value: it
			// falls through to the null branch below, exactly as no footer at all does.
			if (found != parsed.end() && !found->is_null()) {
				crs_source = found->dump();
				break;
			}
		}
	}
	json crs_json = nullptr;
	if (crs_source.empty()) {
		DUCKDB_LOG_WARNING(context,
		                   "cityparquet_write: no CRS for schema '%s' -- the package's footer carries none and none "
		                   "was given (crs => 'EPSG:7415'), so every file's `crs` is written as an explicit null "
		                   "(CRS unknown) and metadata.json declares no projection",
		                   bind_data.schema);
	} else {
		auto projjson = ProjjsonForReferenceSystem(crs_source);
		if (projjson.has_value()) {
			crs_json = json_utils::ParseJson(projjson.value());
		} else {
			try {
				// A carried footer already holds PROJJSON, which the resolver above does
				// not recognise -- it takes referenceSystem spellings, not PROJJSON. Only
				// an object counts: a bare number or string parses as JSON but is not a
				// CRS, and writing it would be the guess the specification forbids.
				auto parsed = json::parse(crs_source);
				if (parsed.is_object()) {
					crs_json = std::move(parsed);
				}
			} catch (const std::exception &) {
				// Not JSON either -- handled as unresolvable just below.
			}
		}
		if (crs_json.is_null()) {
			// A `crs =>` the writer cannot resolve is a bad argument, not an unknowable
			// source CRS: nulling it would quietly defeat the parameter, so it still
			// throws. Anything derived from the package itself falls back to null.
			if (crs_from_parameter) {
				throw InvalidInputException("cityparquet_write: cannot resolve CRS '%s' to PROJJSON", crs_source);
			}
			DUCKDB_LOG_WARNING(context,
			                   "cityparquet_write: cannot resolve the carried CRS '%s' to PROJJSON, so every file's "
			                   "`crs` is written as an explicit null (CRS unknown) rather than guessed",
			                   crs_source);
		}
	}

	// The identifier the GEOMETRY logical type carries. It is the resolved PROJJSON
	// rather than the `crs =>` spelling, because an authority code is not stable
	// through the write: DuckDB's CRS machinery resolves one to PROJJSON when the
	// `spatial` extension happens to be loaded, and leaves it verbatim when it is
	// not, so the same command would produce two different files. PROJJSON is a
	// fixed point under that resolution, and it is what GeoParquet 2.0 asks a writer
	// that can produce it to use.
	const std::string crs_annotation = crs_json.is_null() ? std::string() : crs_json.dump();

	// The extensions the package declares: the union of its object tables' footers, every
	// one of which declares a namespace the same way (cityparquet_merge_extensions keeps
	// it so). Read to give the STAC Item the source spelling of extension classes.
	std::vector<ExtensionDeclaration> declarations;
	for (const auto &entry : carried) {
		json parsed;
		try {
			parsed = json::parse(entry.second);
		} catch (const std::exception &) {
			continue;
		}
		if (!parsed.is_object() || !parsed.contains("extensions") || parsed["extensions"].is_null()) {
			continue;
		}
		for (auto &declaration : DeclarationsFromFooterJson(parsed["extensions"], "cityparquet_write")) {
			const auto known = std::any_of(declarations.begin(), declarations.end(),
			                               [&](const ExtensionDeclaration &d) { return d.ns == declaration.ns; });
			if (!known) {
				declarations.push_back(std::move(declaration));
			}
		}
	}

	// Checked for every table before any file is written, so a refusal leaves the
	// directory as it was.
	for (const auto &table : object_tables) {
		RefusePlusNames(connection, context, bind_data.catalog, bind_data.schema, table, true);
		RefuseNonObjectOther(connection, context, bind_data.catalog, bind_data.schema, table);
	}
	for (const auto &sidecar : sidecars) {
		RefusePlusNames(connection, context, bind_data.catalog, bind_data.schema, sidecar, false);
	}

	// The Hilbert key of every feature, before any table is written: a feature's key
	// comes from its geometry in EVERY object table, and the curve is laid over the
	// extent of the whole package.
	const bool hilbert =
	    bind_data.hilbert && WriteHilbertKeys(connection, context, bind_data.catalog, bind_data.schema, object_tables);

	auto &fs = FileSystem::GetFileSystem(context);
	if (!fs.DirectoryExists(bind_data.directory)) {
		fs.CreateDirectory(bind_data.directory);
	}

	PackageInventory inventory;

	auto write_table = [&](const std::string &table, bool is_object) {
		auto count =
		    Run(connection, "SELECT COUNT(*) FROM " + QualifiedName(bind_data.catalog, bind_data.schema, table));
		const auto rows = count->GetValue(0, 0).GetValue<int64_t>();
		if (rows == 0 && is_object) {
			// "No file for a module with no rows."
			return;
		}
		if (table == "materials") {
			inventory.materials = rows;
		} else if (table == "textures") {
			inventory.textures = rows;
		} else if (table == "implicit_geometries") {
			// A package whose objects carry only implicit geometry has no populated
			// geometry_lod* column in any object table -- its LoDs live here.
			// city3d:lods is the union across every file in the package, so leaving the
			// sidecar out would report an empty LoD set for a perfectly valid package.
			CollectImplicitGeometryInventory(connection, context, bind_data.catalog, bind_data.schema, table,
			                                 inventory);
		}

		std::string kv;
		// Filled from the same facts the `geo` object is built from, so the
		// annotation and the declaration cannot disagree about a column.
		std::set<std::string> legal_geometry;
		std::set<std::string> populated_lods;
		if (is_object) {
			auto facts = CollectFacts(connection, context, bind_data.catalog, bind_data.schema, table);
			for (const auto &fact : facts) {
				populated_lods.insert(fact.name.substr(std::string("geometry_").size()));
				if (fact.Legal()) {
					legal_geometry.insert(fact.name);
				}
			}
			std::vector<std::string> attributes;
			auto columns = Run(connection, "SELECT column_name FROM (DESCRIBE SELECT * FROM " +
			                                   QualifiedName(bind_data.catalog, bind_data.schema, table) + ")");
			for (idx_t row = 0; row < columns->RowCount(); row++) {
				const auto name = columns->GetValue(0, row).ToString();
				if (!IsReservedColumnName(name)) {
					attributes.push_back(name);
				}
			}
			CollectInventory(connection, bind_data.catalog, bind_data.schema, table, rows, facts, attributes,
			                 declarations, inventory);
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
			// A sidecar carries `version` plus only what is meaningful to it. None of the
			// three holds a CRS-bearing coordinate -- relative geometries are stored in
			// local, unplaced coordinates, exempt from the file CRS -- so `crs` is the one
			// state where the key is legitimately ABSENT rather than null (spec
			// 05-metadata.mdx, "CRS rules").
			json city = json::object();
			city["version"] = CITYPARQUET_VERSION;
			kv = "city: " + Literal(city.dump());
		}

		const auto file = table + ".parquet";
		const auto path = fs.JoinPath(bind_data.directory, file);
		// GEOPARQUET_VERSION 'none' suppresses DuckDB's own `geo` key without
		// suppressing the logical type, which follows the column's type rather
		// than this setting. CityParquet's `geo` goes in through KV_METADATA, so
		// the file carries exactly one, and it is this writer's.
		// Whole features move, never single rows, and the rowid tie-break keeps rows of
		// equal key -- a feature's own rows, and every feature without geometry, all of
		// key 0 -- in the table's order: a stable sort, as the reference writer's is.
		std::string order;
		if (is_object && hilbert && HasColumn(context, bind_data.schema, table, "feature_id")) {
			order = " ORDER BY coalesce((SELECT k.hilbert_key FROM " + std::string(HILBERT_KEYS) +
			        " AS k WHERE k.feature_id = __cityparquet_rows.feature_id), 0), __cityparquet_rows.rowid";
		}
		Run(connection, "COPY (SELECT " +
		                    CopySourceList(context, bind_data.schema, table, legal_geometry, crs_annotation,
		                                   is_object ? &populated_lods : nullptr) +
		                    " FROM " + QualifiedName(bind_data.catalog, bind_data.schema, table) +
		                    " AS __cityparquet_rows" + order + ") TO " + Literal(path) +
		                    " (FORMAT PARQUET, GEOPARQUET_VERSION 'none', KV_METADATA {" + kv + "}" +
		                    BloomCopyOptions(is_object, bind_data.bloom) + ");");

		WrittenFile written;
		written.file = file;
		written.action = "written";
		written.rows = rows;
		written.is_object = is_object;
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
		// The Projection extension is declared only when there is a projection to
		// declare. With an unknown CRS there is no PROJJSON to publish and a proj:bbox in
		// an unnamed CRS is uninterpretable, so the Item claims neither rather than
		// declaring an extension it does not use.
		json extensions = json::array({"https://cityjson.github.io/stac-city3d/v0.2.0/schema.json"});
		if (!crs_json.is_null()) {
			extensions.push_back("https://stac-extensions.github.io/projection/v1.1.0/schema.json");
		}
		extensions.push_back("https://stac-extensions.github.io/file/v2.1.0/schema.json");
		item["stac_extensions"] = std::move(extensions);
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
		// The format version is CityParquet's own property; city3d:version is the
		// SOURCE CityJSON version (stac-city3d), taken from a carried footer's
		// source_version when one recorded it -- absent otherwise, never wrong.
		properties["cityparquet:version"] = CITYPARQUET_VERSION;
		for (const auto &entry : carried) {
			json parsed;
			try {
				parsed = json::parse(entry.second);
			} catch (const std::exception &) {
				continue;
			}
			auto found = parsed.find("source_version");
			if (found != parsed.end() && found->is_string()) {
				properties["city3d:version"] = found->get<std::string>();
				break;
			}
		}
		// STAC requires every Item to carry properties.datetime. The honest value is the
		// source's referenceDate, which a footer carries when its writer kept the source
		// metadata (city.other.source_metadata, spec 07-mapping-cityjson.mdx); almost no
		// source has one, so otherwise it is the time this package was written.
		std::string datetime;
		for (const auto &entry : carried) {
			json parsed;
			try {
				parsed = json::parse(entry.second);
			} catch (const std::exception &) {
				continue;
			}
			const auto *date = parsed.is_object() ? &parsed : nullptr;
			for (const char *key : {"other", "source_metadata", "referenceDate"}) {
				if (date == nullptr || !date->is_object() || !date->contains(key)) {
					date = nullptr;
					break;
				}
				date = &(*date)[key];
			}
			if (date != nullptr && date->is_string()) {
				datetime = StacDatetime(date->get<std::string>());
				if (!datetime.empty()) {
					break;
				}
			}
		}
		if (datetime.empty()) {
			date_t day;
			dtime_t time;
			Timestamp::Convert(Timestamp::GetCurrentTimestamp(), day, time);
			datetime = Date::ToString(day) + "T" + Time::ToString(dtime_t(time.micros - time.micros % 1000000)) + "Z";
		}
		properties["datetime"] = datetime;
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
		// Omitted entirely when that CRS is unknown -- the footer states the unknown with
		// an explicit null, but STAC has no null PROJJSON to state it with, and an extent
		// whose CRS nobody knows is a set of numbers a consumer cannot place.
		if (!crs_json.is_null()) {
			properties["proj:projjson"] = crs_json;
			if (inventory.has_extent) {
				properties["proj:bbox"] = json::array({inventory.min_x, inventory.min_y, inventory.min_z,
				                                       inventory.max_x, inventory.max_y, inventory.max_z});
			}
		}
		item["properties"] = std::move(properties);

		json assets = json::object();
		for (const auto &written : state->files) {
			json asset;
			asset["href"] = written.file;
			asset["type"] = "application/vnd.apache.parquet";
			asset["file:size"] = written.bytes;
			// Roles are load-bearing, not decoration: a reader discovers the package's
			// object tables by the `cityparquet-objects` role and its sidecars by
			// `cityparquet-sidecar` (spec 05-metadata.mdx). Without them a package is
			// unreadable by the reference implementation, which reports "package lists
			// no object tables". `data` is the conventional STAC role alongside.
			asset["roles"] = json::array({"data", written.is_object ? "cityparquet-objects" : "cityparquet-sidecar"});
			// No per-asset row count: the spec says a writer SHOULD NOT declare the
			// Table extension merely to publish a row count (05-metadata.mdx); a
			// sidecar's rows are definitions, not city objects, and the package
			// count is city3d:city_objects.
			assets[written.file] = std::move(asset);
		}
		item["assets"] = std::move(assets);

		const auto path = fs.JoinPath(bind_data.directory, "metadata.json");
		auto text = item.dump(2);
		// FILE_CREATE_NEW, not FILE_CREATE: the former truncates, the latter opens an
		// existing file and overwrites from offset 0 only. A rewrite that is shorter than
		// the metadata.json already there would otherwise leave the old tail in place and
		// the file would no longer parse as JSON.
		auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
		fs.Write(*handle, text.data(), static_cast<int64_t>(text.size()));
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

//! Marks text as JSON: the value unchanged, typed as DuckDB's JSON, which the Parquet
//! writer annotates with the JSON logical type. Each value is checked first, since a
//! Parquet reader validates a JSON column and refuses the whole file over one bad cell.
static void JsonFunction(DataChunk &args, ExpressionState &, Vector &result) {
	auto &input = args.data[0];
	UnifiedVectorFormat format;
	input.ToUnifiedFormat(args.size(), format);
	const auto *values = UnifiedVectorFormat::GetData<string_t>(format);
	for (idx_t row = 0; row < args.size(); row++) {
		const auto index = format.sel->get_index(row);
		if (!format.validity.RowIsValid(index)) {
			continue;
		}
		const auto text = values[index];
		if (!json::accept(text.GetData(), text.GetData() + text.GetSize())) {
			const auto shown = text.GetSize() > 80 ? text.GetString().substr(0, 80) + "..." : text.GetString();
			throw InvalidInputException("cityparquet_json: '%s' is not valid JSON", shown);
		}
	}
	result.Reinterpret(input);
}

void RegisterCityParquetWriteFunction(ExtensionLoader &loader) {
	const auto dbl = LogicalType(LogicalTypeId::DOUBLE);
	ScalarFunction hilbert_function("cityparquet_hilbert", {dbl, dbl, dbl, dbl, dbl, dbl},
	                                LogicalType(LogicalTypeId::UINTEGER), HilbertFunction);
	RegisterDocumented(loader, std::move(hilbert_function),
	                   {{"x", "y", "xmin", "ymin", "xmax", "ymax"},
	                    "Returns the position of (x, y) along a 2D Hilbert curve of 2^16 cells per axis laid over "
	                    "the extent [xmin, xmax] x [ymin, ymax]: the row-ordering key cityparquet_write and "
	                    "cityparquet-rs sort features by.",
	                    "cityparquet_hilbert(0.75, 0.25, 0, 0, 1, 1)",
	                    {"cityparquet", "spatial"}});
	ScalarFunction json_function("cityparquet_json", {LogicalType(LogicalTypeId::VARCHAR)}, LogicalType::JSON(),
	                             JsonFunction);
	RegisterDocumented(loader, std::move(json_function),
	                   {{"text"},
	                    "Returns its argument typed as JSON, after checking that it parses, so that a Parquet "
	                    "COPY annotates the column with the JSON logical type; errors on text that is not JSON.",
	                    R"(cityparquet_json('{"roofType": "1000"}'))",
	                    {"cityparquet", "package"}});

	TableFunction func("cityparquet_write", {LogicalType(LogicalTypeId::VARCHAR), LogicalType(LogicalTypeId::VARCHAR)},
	                   WriteScan, WriteBind);
	func.init_global = WriteInitGlobal;
	func.named_parameters["crs"] = LogicalType(LogicalTypeId::VARCHAR);
	func.named_parameters["source_format"] = LogicalType(LogicalTypeId::VARCHAR);
	func.named_parameters["bloom"] = LogicalType(LogicalTypeId::BOOLEAN);
	func.named_parameters["ordering"] = LogicalType(LogicalTypeId::VARCHAR);
	RegisterDocumented(loader, std::move(func),
	                   {{"schema", "directory"},
	                    "Writes a CityParquet package schema out as a package directory: one Parquet file per table "
	                    "with regenerated city and geo footers, plus a metadata.json STAC Item; one row per file.",
	                    "SELECT * FROM cityparquet_write('delft', 'out/', crs => 'EPSG:7415');",
	                    {"cityparquet", "package"}});
}

} // namespace cityjson
} // namespace duckdb

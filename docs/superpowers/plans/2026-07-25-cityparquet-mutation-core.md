# CityParquet Mutation Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship working `cityparquet_delete`, `cityparquet_reconcile`, `cityparquet_validate`, `cityparquet_orphans` and `cityparquet_vacuum` over a CityParquet package the user has loaded into DuckDB tables by hand.

**Architecture:** Each mutation function is a DuckDB `PragmaFunction` registered with a `pragma_query_t` — it *returns SQL text*, which DuckDB parses and executes in place of the pragma, inside the caller's transaction. The extension generates SQL; DuckDB's transaction manager provides atomicity. Read-only inspection functions are ordinary table functions. Every pragma has a scalar `_sql()` twin returning the same text without running it.

**Tech Stack:** C++17, DuckDB extension API (`ScalarFunction`, `PragmaFunction`, `TableFunction`), nlohmann::json, SQL logic tests (`test/sql/*.test`).

## Scope

This is **plan 1 of 2**. It covers everything that works on a hand-loaded package. `insert_cityjson` / `cityparquet_merge` are deferred to plan 2 because they depend on appearance normalisation (dataset-global sidecar ids, inlined texture UVs) and package I/O (`cityparquet_read` / `cityparquet_write`), which are substantial in their own right. Delete and reconcile need neither, so they ship first.

Design: `docs/superpowers/specs/2026-07-25-cityparquet-mutation-functions-design.md`.

## Global Constraints

- C++17. The extension is C++, not Rust.
- Every new `.cpp` must be added to `EXTENSION_SOURCES` in `CMakeLists.txt`.
- Build: `cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest`
- Test one file: `./build/release/test/unittest --test-dir . "test/sql/<name>.test"`
- Test all: `./build/release/test/unittest --test-dir . "test/sql/*"`
- Tests are SQL logic tests in `test/sql/*.test`. There is no C++ unit-test harness in this repo. Every task is red/green: write the failing `.test` first, run it, see it fail, then implement.
- Headers go in `src/include/cityjson/`, implementations in `src/cityjson/`, both inside `namespace duckdb { namespace cityjson { … } }`.
- **The `bbox` struct field names are `min_x, min_y, min_z, max_x, max_y, max_z`** (see `src/cityjson/column_types.cpp:127-132`). The CityParquet spec writes them as `xmin…zmax`. This plan matches the existing codebase, not the spec. The divergence is real and is flagged in Task 8 for the user to resolve upstream; do not silently rename either side.
- Generated SQL must quote identifiers with `KeywordHelper::WriteOptionallyQuoted` and string literals with `SQLString` / `KeywordHelper::WriteQuoted`. Never concatenate a user-supplied schema name unquoted.
- Fixture with real hierarchy: `test/data/delft_subset.city.jsonl` — `Building` rows with one `BuildingPart` child each, `children_roles` all NULL.

---

### Task 1: `cityjson_wkb_extent` scalar function

The 3D extent of a WKB blob. Every `bbox` recomputation in later tasks depends on it. DuckDB `spatial` cannot substitute: it raises `Unsupported geometry type in WKB` on `PolyhedralSurfaceZ`, which is what every CityParquet solid LoD is.

**Files:**
- Create: `src/include/cityjson/wkb_extent.hpp`
- Create: `src/cityjson/wkb_extent.cpp`
- Modify: `CMakeLists.txt` — add `src/cityjson/wkb_extent.cpp` to `EXTENSION_SOURCES` after `src/cityjson/wkb_decoder.cpp`
- Modify: `src/cityjson_extension.cpp` — call `cityjson::RegisterWKBExtentFunction(loader);` in `LoadInternal`
- Test: `test/sql/cityjson_wkb_extent.test`

**Interfaces:**
- Consumes: `WKBDecoder::Decode(const uint8_t*, size_t) -> WKBDecodeResult` (`src/include/cityjson/wkb_decoder.hpp`), whose `.boundaries` is nested json with `[x,y,z]` leaf arrays.
- Produces: SQL scalar `cityjson_wkb_extent(BLOB) -> STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE, max_x DOUBLE, max_y DOUBLE, max_z DOUBLE)`, NULL-in/NULL-out. C++ entry point `void RegisterWKBExtentFunction(ExtensionLoader &loader);` in `cityjson/wkb_extent.hpp`.

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityjson_wkb_extent.test`:

```
# name: test/sql/cityjson_wkb_extent.test
# description: cityjson_wkb_extent — 3D extent of a WKB blob, solids included
# group: [sql]

require cityjson

statement ok
CREATE TABLE b AS SELECT * FROM read_cityjson('test/data/minimal.city.json');

# The struct shape matches the existing bbox column exactly, so generated SQL
# can assign one to the other.
query T
SELECT typeof(cityjson_wkb_extent(geometry_lod2_2)) FROM b WHERE geometry_lod2_2 IS NOT NULL;
----
STRUCT(min_x DOUBLE, min_y DOUBLE, min_z DOUBLE, max_x DOUBLE, max_y DOUBLE, max_z DOUBLE)

# It agrees with the bbox the reader already computed for the same row.
query I
SELECT COUNT(*) FROM b
WHERE geometry_lod2_2 IS NOT NULL
  AND cityjson_wkb_extent(geometry_lod2_2) IS NOT DISTINCT FROM bbox;
----
1

# NULL in, NULL out — bbox recomputation relies on this for rows with no
# geometry at a given LoD.
query I
SELECT cityjson_wkb_extent(NULL::BLOB) IS NULL;
----
true

# Solid geometry (PolyhedralSurfaceZ) works — this is the case DuckDB spatial
# rejects, and it is most of the interesting data.
statement ok
CREATE TABLE d AS SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl');

query I
SELECT COUNT(*) FROM d
WHERE geometry_lod2_2 IS NOT NULL
  AND cityjson_wkb_extent(geometry_lod2_2).min_z < cityjson_wkb_extent(geometry_lod2_2).max_z;
----
20

statement error
SELECT cityjson_wkb_extent('not wkb'::BLOB);
----
cityjson_wkb_extent
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
./build/release/test/unittest --test-dir . "test/sql/cityjson_wkb_extent.test"
```

Expected: failure — `Catalog Error: Scalar Function with name cityjson_wkb_extent does not exist`.

If the `COUNT(*)` on the delft fixture is not `20`, correct the expected number to whatever the fixture actually yields — run `./build/release/duckdb -c "SELECT COUNT(*) FROM read_cityjsonseq('test/data/delft_subset.city.jsonl') WHERE geometry_lod2_2 IS NOT NULL;"` and use that. Do not change the assertion's *meaning*.

- [ ] **Step 3: Write the header**

Create `src/include/cityjson/wkb_extent.hpp`:

```cpp
#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

//! Registers cityjson_wkb_extent(BLOB) -> STRUCT(min_x, min_y, min_z, max_x, max_y, max_z).
//! Field names deliberately match the `bbox` column built in column_types.cpp so a
//! recomputed extent can be assigned straight into it.
void RegisterWKBExtentFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 4: Write the implementation**

Create `src/cityjson/wkb_extent.cpp`:

```cpp
#include "cityjson/wkb_extent.hpp"
#include "cityjson/wkb_decoder.hpp"
#include "cityjson/error.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/scalar_function.hpp"

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
// Recurse until a leaf is reached rather than switching on geometry type, so this
// covers every type the decoder supports — MultiPoint through PolyhedralSurfaceZ
// and GeometryCollectionZ — without a per-type branch to keep in sync.
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
	auto count = args.size();
	auto &input = args.data[0];

	UnifiedVectorFormat format;
	input.ToUnifiedFormat(count, format);
	auto blobs = UnifiedVectorFormat::GetData<string_t>(format);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &children = StructVector::GetEntries(result);
	auto min_x = FlatVector::GetData<double>(*children[0]);
	auto min_y = FlatVector::GetData<double>(*children[1]);
	auto min_z = FlatVector::GetData<double>(*children[2]);
	auto max_x = FlatVector::GetData<double>(*children[3]);
	auto max_y = FlatVector::GetData<double>(*children[4]);
	auto max_z = FlatVector::GetData<double>(*children[5]);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto idx = format.sel->get_index(i);
		if (!format.validity.RowIsValid(idx)) {
			result_validity.SetInvalid(i);
			continue;
		}
		auto &blob = blobs[idx];
		Extent3D extent;
		try {
			auto decoded = WKBDecoder::Decode(const_cast<const uint8_t *>(
			                                      reinterpret_cast<const uint8_t *>(blob.GetData())),
			                                  blob.GetSize());
			Accumulate(decoded.boundaries, extent);
		} catch (const std::exception &e) {
			throw InvalidInputException("cityjson_wkb_extent: cannot decode WKB: %s", e.what());
		}
		if (extent.Empty()) {
			// Structurally valid WKB carrying no coordinates (e.g. an empty
			// collection) has no extent; NULL is the honest answer, not zeros.
			result_validity.SetInvalid(i);
			continue;
		}
		min_x[i] = extent.min_x;
		min_y[i] = extent.min_y;
		min_z[i] = extent.min_z;
		max_x[i] = extent.max_x;
		max_y[i] = extent.max_y;
		max_z[i] = extent.max_z;
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

LogicalType ExtentType() {
	child_list_t<LogicalType> children;
	children.push_back(std::make_pair("min_x", LogicalType::DOUBLE));
	children.push_back(std::make_pair("min_y", LogicalType::DOUBLE));
	children.push_back(std::make_pair("min_z", LogicalType::DOUBLE));
	children.push_back(std::make_pair("max_x", LogicalType::DOUBLE));
	children.push_back(std::make_pair("max_y", LogicalType::DOUBLE));
	children.push_back(std::make_pair("max_z", LogicalType::DOUBLE));
	return LogicalType::STRUCT(children);
}

} // namespace

void RegisterWKBExtentFunction(ExtensionLoader &loader) {
	ScalarFunction func("cityjson_wkb_extent", {LogicalType::BLOB}, ExtentType(), WKBExtentFunction);
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 5: Wire it into the build and the loader**

In `CMakeLists.txt`, in `EXTENSION_SOURCES`, immediately after `src/cityjson/wkb_decoder.cpp`:

```cmake
    src/cityjson/wkb_extent.cpp
```

In `src/cityjson_extension.cpp`, add the include near the other `cityjson/` includes:

```cpp
#include "cityjson/wkb_extent.hpp"
```

and inside `LoadInternal`, after the geoparquet registration:

```cpp
	// Register cityjson_wkb_extent (3D extent of a WKB blob, solid family included)
	cityjson::RegisterWKBExtentFunction(loader);
```

- [ ] **Step 6: Build**

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest
```

- [ ] **Step 7: Run the test and confirm it passes**

```bash
./build/release/test/unittest --test-dir . "test/sql/cityjson_wkb_extent.test"
```

Expected: PASS.

- [ ] **Step 8: Run the full suite to confirm nothing regressed**

```bash
./build/release/test/unittest --test-dir . "test/sql/*"
```

- [ ] **Step 9: Commit**

```bash
git add src/include/cityjson/wkb_extent.hpp src/cityjson/wkb_extent.cpp \
        src/cityjson_extension.cpp CMakeLists.txt test/sql/cityjson_wkb_extent.test
git commit -m "feat(cityparquet): cityjson_wkb_extent scalar for 3D WKB extents

Needed by every bbox recomputation in the mutation functions. DuckDB
spatial cannot substitute: it rejects PolyhedralSurfaceZ, which is what
every CityParquet solid LoD is. Struct field names match the existing
bbox column so a recomputed extent assigns straight into it."
```

---

### Task 2: `__cityparquet` bookkeeping and `cityparquet_init`

The package's per-file provenance table. Later tasks read it to know which tables are object tables versus sidecars; the plan-2 writer reads it for the `city` footer values.

**Files:**
- Create: `src/include/cityjson/cityparquet_package.hpp`
- Create: `src/cityjson/cityparquet_package.cpp`
- Modify: `CMakeLists.txt` — add `src/cityjson/cityparquet_package.cpp` under a new `# CityParquet package layer` comment after the COPY TO block
- Modify: `src/cityjson_extension.cpp` — `cityjson::RegisterCityParquetPackageFunctions(loader);`
- Test: `test/sql/cityparquet_init.test`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - SQL: `PRAGMA cityparquet_init(schema VARCHAR)`, and scalar `cityparquet_init_sql(schema VARCHAR) -> VARCHAR`.
  - C++ in `cityjson/cityparquet_package.hpp`:
    - `void RegisterCityParquetPackageFunctions(ExtensionLoader &loader);`
    - `const std::vector<std::string> &ModuleTableNames();` — the 11 standard module basenames, in spec order.
    - `const std::vector<std::string> &SidecarTableNames();` — `{"materials","textures","geometry_templates"}`.
    - `std::vector<std::string> ObjectTablesInSchema(ClientContext &context, const std::string &schema);` — tables present in `schema` whose name is in `ModuleTableNames()`, sorted; throws `BinderException` if the schema has none.
    - `std::string QualifiedName(const std::string &schema, const std::string &table);` — properly quoted `"schema"."table"`.
    - `std::vector<std::string> GeometryLodColumns(ClientContext &context, const std::string &schema, const std::string &table);` — the `geometry_lod*` column names of one table, in catalog order. Tasks 3 and 4 both consume this, so it lives here rather than in either of them.
    - `std::vector<std::string> AppearanceLodColumns(ClientContext &context, const std::string &schema, const std::string &table, const std::string &prefix);` — same, for `material_lod*` / `texture_lod*`; `prefix` is `"material_lod"` or `"texture_lod"`.
    - `std::string AllObjectsCTE(const std::string &schema, const std::vector<std::string> &object_tables);` — the `all_objects AS (…)` CTE body unioning every object table's identity and hierarchy columns. Tasks 3, 4 and 5 all build on it.
  - `__cityparquet` schema: `table_name VARCHAR, file_name VARCHAR, role VARCHAR, city JSON`. `role` is `'object'` or `'sidecar'`.

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityparquet_init.test`:

```
# name: test/sql/cityparquet_init.test
# description: cityparquet_init — package bookkeeping over hand-loaded tables
# group: [sql]

require cityjson

statement ok
CREATE SCHEMA ams;

statement ok
CREATE TABLE ams.building AS
  SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl');

statement ok
PRAGMA cityparquet_init('ams');

# One bookkeeping row per package table, roles assigned by name convention.
query TTT
SELECT table_name, file_name, role FROM ams.__cityparquet ORDER BY table_name;
----
building	building.parquet	object

# A hand-rolled load has discarded the footer, so city is NULL and the plan-2
# writer will require crs to be supplied explicitly.
query I
SELECT city IS NULL FROM ams.__cityparquet WHERE table_name = 'building';
----
true

# Sidecars are recognised by name and get role 'sidecar'.
statement ok
CREATE TABLE ams.materials (id BIGINT, name VARCHAR);

statement ok
PRAGMA cityparquet_init('ams');

query TT
SELECT table_name, role FROM ams.__cityparquet ORDER BY table_name;
----
building	object
materials	sidecar

# Re-running is idempotent — it does not duplicate rows.
query I
SELECT COUNT(*) FROM ams.__cityparquet;
----
2

# The _sql twin returns the text without running it.
query I
SELECT cityparquet_init_sql('ams') LIKE '%CREATE TABLE IF NOT EXISTS%__cityparquet%';
----
true

# A schema with no object table is not a package.
statement ok
CREATE SCHEMA empty_pkg;

statement error
PRAGMA cityparquet_init('empty_pkg');
----
no CityParquet object table
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
./build/release/test/unittest --test-dir . "test/sql/cityparquet_init.test"
```

Expected: `Catalog Error` — the pragma does not exist.

- [ ] **Step 3: Write the header**

Create `src/include/cityjson/cityparquet_package.hpp`:

```cpp
#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! The 11 CityGML 3.0 modules that hold feature objects, as snake_case table
//! basenames, in the order the specification lists them.
const std::vector<std::string> &ModuleTableNames();

//! materials, textures, geometry_templates.
const std::vector<std::string> &SidecarTableNames();

//! Object tables actually present in `schema`, sorted. Throws BinderException when
//! the schema contains none — that is not a CityParquet package.
std::vector<std::string> ObjectTablesInSchema(ClientContext &context, const std::string &schema);

//! Sidecar tables actually present in `schema`, sorted. May be empty.
std::vector<std::string> SidecarTablesInSchema(ClientContext &context, const std::string &schema);

//! Quoted "schema"."table", safe to concatenate into generated SQL.
std::string QualifiedName(const std::string &schema, const std::string &table);

//! geometry_lod* column names of one table, in catalog order. Never hard-code an LoD
//! set: which LoDs exist is a property of the dataset.
std::vector<std::string> GeometryLodColumns(ClientContext &context, const std::string &schema,
                                            const std::string &table);

//! As above for appearance columns; `prefix` is "material_lod" or "texture_lod".
std::vector<std::string> AppearanceLodColumns(ClientContext &context, const std::string &schema,
                                              const std::string &table, const std::string &prefix);

//! The `all_objects AS (...)` CTE body: every object table's identity and hierarchy
//! columns, unioned. Those columns are common to every module, so the consistency
//! checks and re-derivations can be written once rather than once per module.
std::string AllObjectsCTE(const std::string &schema, const std::vector<std::string> &object_tables);

//! Registers cityparquet_init (pragma) and cityparquet_init_sql (scalar).
void RegisterCityParquetPackageFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 4: Write the implementation**

Create `src/cityjson/cityparquet_package.cpp`:

```cpp
#include "cityjson/cityparquet_package.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <algorithm>
#include <set>

namespace duckdb {
namespace cityjson {

const std::vector<std::string> &ModuleTableNames() {
	static const std::vector<std::string> names = {
	    "building", "bridge",  "tunnel",     "construction",  "transportation", "vegetation",
	    "relief",   "water_body", "land_use", "city_furniture", "generics"};
	return names;
}

const std::vector<std::string> &SidecarTableNames() {
	static const std::vector<std::string> names = {"materials", "textures", "geometry_templates"};
	return names;
}

std::string QualifiedName(const std::string &schema, const std::string &table) {
	return KeywordHelper::WriteOptionallyQuoted(schema) + "." + KeywordHelper::WriteOptionallyQuoted(table);
}

namespace {

//! Names of tables that exist in `schema`, lower-cased.
std::set<std::string> TablesInSchema(ClientContext &context, const std::string &schema) {
	std::set<std::string> present;
	auto &schema_entry = Catalog::GetSchema(context, INVALID_CATALOG, schema);
	schema_entry.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		if (entry.type == CatalogType::TABLE_ENTRY) {
			present.insert(StringUtil::Lower(entry.name));
		}
	});
	return present;
}

std::vector<std::string> Intersect(const std::set<std::string> &present, const std::vector<std::string> &candidates) {
	std::vector<std::string> found;
	for (const auto &name : candidates) {
		if (present.count(name) > 0) {
			found.push_back(name);
		}
	}
	std::sort(found.begin(), found.end());
	return found;
}

} // namespace

std::vector<std::string> ObjectTablesInSchema(ClientContext &context, const std::string &schema) {
	auto found = Intersect(TablesInSchema(context, schema), ModuleTableNames());
	if (found.empty()) {
		throw BinderException("cityparquet: schema '%s' has no CityParquet object table "
		                      "(expected one of building, transportation, ...)",
		                      schema);
	}
	return found;
}

std::vector<std::string> SidecarTablesInSchema(ClientContext &context, const std::string &schema) {
	return Intersect(TablesInSchema(context, schema), SidecarTableNames());
}

namespace {

//! Column names of `schema.table` beginning with `prefix`, in catalog order.
std::vector<std::string> ColumnsWithPrefix(ClientContext &context, const std::string &schema,
                                           const std::string &table, const std::string &prefix) {
	std::vector<std::string> found;
	auto &entry = Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, schema, table);
	for (auto &column : entry.GetColumns().Logical()) {
		if (StringUtil::StartsWith(StringUtil::Lower(column.Name()), prefix)) {
			found.push_back(column.Name());
		}
	}
	return found;
}

} // namespace

std::vector<std::string> GeometryLodColumns(ClientContext &context, const std::string &schema,
                                            const std::string &table) {
	// "geometry_lod" also prefixes "geometry_properties_lod"? No — that begins
	// "geometry_p", so a plain prefix test is sufficient and unambiguous.
	return ColumnsWithPrefix(context, schema, table, "geometry_lod");
}

std::vector<std::string> AppearanceLodColumns(ClientContext &context, const std::string &schema,
                                              const std::string &table, const std::string &prefix) {
	return ColumnsWithPrefix(context, schema, table, prefix);
}

std::string AllObjectsCTE(const std::string &schema, const std::vector<std::string> &object_tables) {
	std::string cte = "all_objects AS (\n";
	for (idx_t i = 0; i < object_tables.size(); i++) {
		if (i > 0) {
			cte += "  UNION ALL\n";
		}
		cte += "  SELECT " + SQLString(object_tables[i]) +
		       " AS __tbl, id, feature_id, parents, children, children_roles FROM " +
		       QualifiedName(schema, object_tables[i]) + "\n";
	}
	cte += ")";
	return cte;
}

namespace {

std::string BuildInitSQL(ClientContext &context, const std::string &schema) {
	auto object_tables = ObjectTablesInSchema(context, schema);
	auto sidecar_tables = SidecarTablesInSchema(context, schema);

	auto bookkeeping = QualifiedName(schema, "__cityparquet");
	std::string sql;
	sql += "CREATE TABLE IF NOT EXISTS " + bookkeeping +
	       " (table_name VARCHAR, file_name VARCHAR, role VARCHAR, city JSON);\n";

	// Idempotent: re-running must not duplicate rows, and must pick up tables
	// created since the last call.
	auto emit = [&](const std::string &table, const char *role) {
		sql += "INSERT INTO " + bookkeeping + " (table_name, file_name, role, city) SELECT " +
		       SQLString(table) + ", " + SQLString(table + ".parquet") + ", " + SQLString(role) +
		       ", NULL WHERE NOT EXISTS (SELECT 1 FROM " + bookkeeping + " WHERE table_name = " + SQLString(table) +
		       ");\n";
	};
	for (const auto &table : object_tables) {
		emit(table, "object");
	}
	for (const auto &table : sidecar_tables) {
		emit(table, "sidecar");
	}
	return sql;
}

std::string PragmaInit(ClientContext &context, const FunctionParameters &parameters) {
	return BuildInitSQL(context, parameters.values[0].ToString());
}

void InitSQLScalar(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t schema) {
		return StringVector::AddString(result, BuildInitSQL(context, schema.GetString()));
	});
}

} // namespace

void RegisterCityParquetPackageFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(PragmaFunction::PragmaCall("cityparquet_init", PragmaInit, {LogicalType::VARCHAR}));

	ScalarFunction init_sql("cityparquet_init_sql", {LogicalType::VARCHAR}, LogicalType::VARCHAR, InitSQLScalar);
	loader.RegisterFunction(init_sql);
}

} // namespace cityjson
} // namespace duckdb
```

Add `#include "duckdb/common/vector_operations/unary_executor.hpp"` if `UnaryExecutor` is not already reachable, and `#include "duckdb/common/types/value.hpp"` for `SQLString`.

- [ ] **Step 5: Wire it into the build and the loader**

`CMakeLists.txt`, after the `# COPY TO` block:

```cmake
    # CityParquet package layer
    src/cityjson/cityparquet_package.cpp
```

`src/cityjson_extension.cpp`: include `"cityjson/cityparquet_package.hpp"` and call in `LoadInternal`:

```cpp
	// Register CityParquet package bookkeeping (cityparquet_init)
	cityjson::RegisterCityParquetPackageFunctions(loader);
```

- [ ] **Step 6: Build, run the test, confirm PASS**

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest
./build/release/test/unittest --test-dir . "test/sql/cityparquet_init.test"
```

- [ ] **Step 7: Run the full suite**

```bash
./build/release/test/unittest --test-dir . "test/sql/*"
```

- [ ] **Step 8: Commit**

```bash
git add src/include/cityjson/cityparquet_package.hpp src/cityjson/cityparquet_package.cpp \
        src/cityjson_extension.cpp CMakeLists.txt test/sql/cityparquet_init.test
git commit -m "feat(cityparquet): package bookkeeping and cityparquet_init

A package is a DuckDB schema whose tables are named by the spec's file
basenames. __cityparquet records one row per package file, which is the
only thing a hand-rolled read_parquet load does not give you."
```

---

### Task 3: `cityparquet_validate` and `cityparquet_orphans`

Read-only. Built before the mutating functions because they are the oracle those functions' tests assert against.

**Files:**
- Create: `src/include/cityjson/cityparquet_validate.hpp`
- Create: `src/cityjson/cityparquet_validate.cpp`
- Modify: `CMakeLists.txt`, `src/cityjson_extension.cpp`
- Test: `test/sql/cityparquet_validate.test`

**Interfaces:**
- Consumes: `ObjectTablesInSchema`, `SidecarTablesInSchema`, `QualifiedName` from Task 2.
- Produces:
  - `cityparquet_validate(schema VARCHAR)` table function → columns `check VARCHAR, severity VARCHAR, table_name VARCHAR, object_id VARCHAR, message VARCHAR`.
  - `cityparquet_orphans(schema VARCHAR)` table function → columns `table_name VARCHAR, id VARCHAR, reason VARCHAR`.
  - `void RegisterCityParquetValidateFunctions(ExtensionLoader &loader);`

Both are implemented as pragma-style SQL generation executed through a bound `TableFunction` that simply delegates to a generated `SELECT`. The simplest correct shape, and the one this task uses: register them as **pragma functions returning a `SELECT`**, so `cityparquet_validate` is invoked as `PRAGMA cityparquet_validate('ams')` and its result is the `SELECT`'s result. This avoids implementing a scanning table function that would have to re-execute SQL internally.

Checks emitted, each as one row per violation:

| `check` | `severity` | Condition |
|---|---|---|
| `feature_id_null` | `error` | `feature_id IS NULL` |
| `feature_id_dangling` | `error` | `feature_id` not present as any object's `id` |
| `parent_dangling` | `error` | an entry of `parents` matches no `id` in the package |
| `child_dangling` | `error` | an entry of `children` matches no `id` in the package |
| `children_roles_misaligned` | `error` | `children_roles IS NOT NULL AND len(children_roles) <> len(children)` |
| `id_duplicate` | `error` | the same `id` appears more than once across all object tables |
| `bbox_stale` | `warning` | `bbox` differs from the union of the row's own `cityjson_wkb_extent(geometry_lod*)` and its children's `bbox` |

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityparquet_validate.test`:

```
# name: test/sql/cityparquet_validate.test
# description: cityparquet_validate / cityparquet_orphans — the consistency oracle
# group: [sql]

require cityjson

statement ok
CREATE SCHEMA ams;

statement ok
CREATE TABLE ams.building AS
  SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl');

statement ok
PRAGMA cityparquet_init('ams');

# A freshly loaded, untouched package is clean.
query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_validate('ams')) WHERE severity = 'error';
----
0

# Break the hierarchy: delete a BuildingPart without touching its parent's
# children array. The parent is now left with a dangling child reference.
statement ok
DELETE FROM ams.building WHERE object_type = 'BuildingPart'
  AND id = 'NL.IMBAG.Pand.0503100000012869-0';

query TT
SELECT check, severity FROM (PRAGMA cityparquet_validate('ams'))
WHERE check = 'child_dangling';
----
child_dangling	error

# Break feature_id: point a row at a feature family that does not exist.
statement ok
UPDATE ams.building SET feature_id = 'no-such-feature'
  WHERE id = 'NL.IMBAG.Pand.0503100000016459';

query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_validate('ams'))
WHERE check = 'feature_id_dangling';
----
1

# Misaligned children_roles is an error even when both are non-null.
statement ok
UPDATE ams.building SET children_roles = ['a', 'b']
  WHERE id = 'NL.IMBAG.Pand.0503100000005156';

query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_validate('ams'))
WHERE check = 'children_roles_misaligned';
----
1

# Orphaned sidecar rows: a material no material_lod* cell references.
statement ok
CREATE TABLE ams.materials (id BIGINT, name VARCHAR);

statement ok
INSERT INTO ams.materials VALUES (1, 'unreferenced');

statement ok
PRAGMA cityparquet_init('ams');

query TTT
SELECT table_name, id, reason FROM (PRAGMA cityparquet_orphans('ams'));
----
materials	1	unreferenced
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
./build/release/test/unittest --test-dir . "test/sql/cityparquet_validate.test"
```

Expected: `Catalog Error` on `cityparquet_validate`.

- [ ] **Step 3: Write the header**

Create `src/include/cityjson/cityparquet_validate.hpp`:

```cpp
#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>

namespace duckdb {
namespace cityjson {

//! SELECT returning (check, severity, table_name, object_id, message) — one row per
//! consistency violation in `schema`. Returns a zero-row SELECT when the package is clean.
std::string BuildValidateSQL(ClientContext &context, const std::string &schema);

//! SELECT returning (table_name, id, reason) — one row per unreferenced sidecar row.
std::string BuildOrphansSQL(ClientContext &context, const std::string &schema);

//! A scalar sub-SELECT yielding every sidecar id currently referenced from the object
//! tables of `schema`, for the given sidecar table. Shared by BuildOrphansSQL (which
//! reports the complement) and BuildVacuumSQL (which deletes it), so the reporter and
//! the deleter cannot drift apart.
std::string ReferencedIds(ClientContext &context, const std::string &schema, const std::string &sidecar);

//! DELETE statements removing every unreferenced sidecar row from `schema`.
std::string BuildVacuumSQL(ClientContext &context, const std::string &schema);

void RegisterCityParquetValidateFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 4: Write the implementation**

Create `src/cityjson/cityparquet_validate.cpp`. Build the SQL by `UNION ALL`-ing one sub-select per check, over a CTE that unions every object table's identity columns:

```cpp
#include "cityjson/cityparquet_validate.hpp"
#include "cityjson/cityparquet_package.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <vector>

namespace duckdb {
namespace cityjson {

std::string BuildValidateSQL(ClientContext &context, const std::string &schema) {
	auto object_tables = ObjectTablesInSchema(context, schema);

	std::vector<std::string> checks;

	checks.push_back("SELECT 'feature_id_null' AS check, 'error' AS severity, __tbl AS table_name, "
	                 "id AS object_id, 'feature_id is NULL' AS message "
	                 "FROM all_objects WHERE feature_id IS NULL");

	checks.push_back("SELECT 'feature_id_dangling', 'error', __tbl, id, "
	                 "'feature_id ' || feature_id || ' matches no object id' "
	                 "FROM all_objects WHERE feature_id IS NOT NULL "
	                 "AND feature_id NOT IN (SELECT id FROM all_objects)");

	checks.push_back("SELECT 'parent_dangling', 'error', __tbl, id, "
	                 "'parent ' || p || ' matches no object id' "
	                 "FROM all_objects, UNNEST(parents) AS t(p) "
	                 "WHERE p IS NOT NULL AND p NOT IN (SELECT id FROM all_objects)");

	checks.push_back("SELECT 'child_dangling', 'error', __tbl, id, "
	                 "'child ' || c || ' matches no object id' "
	                 "FROM all_objects, UNNEST(children) AS t(c) "
	                 "WHERE c IS NOT NULL AND c NOT IN (SELECT id FROM all_objects)");

	checks.push_back("SELECT 'children_roles_misaligned', 'error', __tbl, id, "
	                 "'children_roles length does not match children' "
	                 "FROM all_objects WHERE children_roles IS NOT NULL "
	                 "AND len(children_roles) <> COALESCE(len(children), 0)");

	checks.push_back("SELECT 'id_duplicate', 'error', ANY_VALUE(__tbl), id, "
	                 "'id appears ' || COUNT(*) || ' times in the package' "
	                 "FROM all_objects GROUP BY id HAVING COUNT(*) > 1");

	std::string sql = "WITH " + AllObjectsCTE(schema, object_tables) + "\n";
	sql += StringUtil::Join(checks, "\nUNION ALL\n");
	sql += "\nORDER BY severity, check, object_id;";
	return sql;
}

std::string BuildOrphansSQL(ClientContext &context, const std::string &schema) {
	auto object_tables = ObjectTablesInSchema(context, schema);
	auto sidecars = SidecarTablesInSchema(context, schema);

	if (sidecars.empty()) {
		return "SELECT NULL::VARCHAR AS table_name, NULL::VARCHAR AS id, NULL::VARCHAR AS reason WHERE false;";
	}
	std::vector<std::string> parts;
	for (const auto &sidecar : sidecars) {
		parts.push_back("SELECT " + SQLString(sidecar) + " AS table_name, CAST(s.id AS VARCHAR) AS id, " +
		                "'unreferenced' AS reason FROM " + QualifiedName(schema, sidecar) + " s WHERE s.id NOT IN (" +
		                ReferencedIds(context, schema, sidecar) + ")");
	}
	return StringUtil::Join(parts, "\nUNION ALL\n") + "\nORDER BY table_name, id;";
}

namespace {

std::string PragmaValidate(ClientContext &context, const FunctionParameters &parameters) {
	return BuildValidateSQL(context, parameters.values[0].ToString());
}

std::string PragmaOrphans(ClientContext &context, const FunctionParameters &parameters) {
	return BuildOrphansSQL(context, parameters.values[0].ToString());
}

} // namespace

void RegisterCityParquetValidateFunctions(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    PragmaFunction::PragmaCall("cityparquet_validate", PragmaValidate, {LogicalType::VARCHAR}));
	loader.RegisterFunction(PragmaFunction::PragmaCall("cityparquet_orphans", PragmaOrphans, {LogicalType::VARCHAR}));
}

} // namespace cityjson
} // namespace duckdb
```

`ReferencedIds(context, schema, sidecar)` returns a sub-`SELECT` of the ids currently in use, built by unioning one term per (object table × appearance column), with the columns discovered via `AppearanceLodColumns` from Task 2 — never a hard-coded LoD list.

- **`materials`** — for each `material_lod*` column `c`, the per-face form `SELECT UNNEST(json_extract(c, '$.*.values')::BIGINT[])` and the whole-geometry form `SELECT json_extract(c, '$.*.value')::BIGINT`, both filtered `WHERE c IS NOT NULL`.
- **`textures`** — for each `texture_lod*` column, the texture id is the **first element of each ring array**, so the term reaches `'$.*.values[*][*][0]'` rather than the whole array.
- **`geometry_templates`** — no JSON involved: `SELECT template.id FROM <object table> WHERE template IS NOT NULL`.

Confirm the exact `json_extract` paths against a real cell before relying on them:

```bash
./build/release/duckdb -c "SELECT material_lod3_0 FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl') WHERE material_lod3_0 IS NOT NULL LIMIT 1;"
```

If a path does not match the cell shape the reader actually emits, fix the path — not the test.

- [ ] **Step 5: Wire into build and loader, build, run the test**

Add `src/cityjson/cityparquet_validate.cpp` to `EXTENSION_SOURCES`; include the header and call `cityjson::RegisterCityParquetValidateFunctions(loader);` in `LoadInternal`.

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest
./build/release/test/unittest --test-dir . "test/sql/cityparquet_validate.test"
```

- [ ] **Step 6: Run the full suite and commit**

```bash
./build/release/test/unittest --test-dir . "test/sql/*"
git add src/include/cityjson/cityparquet_validate.hpp src/cityjson/cityparquet_validate.cpp \
        src/cityjson_extension.cpp CMakeLists.txt test/sql/cityparquet_validate.test
git commit -m "feat(cityparquet): cityparquet_validate and cityparquet_orphans

Read-only consistency checks over a package. Built before the mutating
functions because they are the oracle those functions' tests assert
against."
```

---

### Task 4: `cityparquet_reconcile`

Re-derives `feature_id`, reciprocal hierarchy, and `bbox` — the three things a raw SQL edit invalidates. This is Phase 6 of the design's insert algorithm, standalone, and the mutating primitive Task 5 reuses.

**Files:**
- Create: `src/include/cityjson/cityparquet_reconcile.hpp`
- Create: `src/cityjson/cityparquet_reconcile.cpp`
- Modify: `CMakeLists.txt`, `src/cityjson_extension.cpp`
- Test: `test/sql/cityparquet_reconcile.test`

**Interfaces:**
- Consumes: `ObjectTablesInSchema`, `QualifiedName` (Task 2); `cityjson_wkb_extent` (Task 1); `cityparquet_validate` (Task 3) for the test's assertions.
- Produces:
  - `PRAGMA cityparquet_reconcile(schema VARCHAR [, checks := VARCHAR[]])`
  - scalar `cityparquet_reconcile_sql(schema VARCHAR) -> VARCHAR`
  - `std::string BuildReconcileSQL(ClientContext &context, const std::string &schema, const std::vector<std::string> &checks);` — `checks` empty means all three.
  - `std::vector<std::string> GeometryLodColumns(ClientContext &context, const std::string &schema, const std::string &table);` — the `geometry_lod*` column names of one table, by catalog lookup. Task 3 and plan 2 reuse this.

Order within the generated script is normative and must not be rearranged: `feature_id` → hierarchy → `bbox`. `bbox` depends on the corrected hierarchy.

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityparquet_reconcile.test`:

```
# name: test/sql/cityparquet_reconcile.test
# description: cityparquet_reconcile — re-derive feature_id, hierarchy, bbox
# group: [sql]

require cityjson

statement ok
CREATE SCHEMA ams;

statement ok
CREATE TABLE ams.building AS
  SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl');

statement ok
PRAGMA cityparquet_init('ams');

# Corrupt feature_id on a child, then reconcile it back to its root parent.
statement ok
UPDATE ams.building SET feature_id = 'wrong'
  WHERE id = 'NL.IMBAG.Pand.0503100000012869-0';

statement ok
PRAGMA cityparquet_reconcile('ams');

query T
SELECT feature_id FROM ams.building WHERE id = 'NL.IMBAG.Pand.0503100000012869-0';
----
NL.IMBAG.Pand.0503100000012869

# A root object's feature_id equals its own id.
query I
SELECT COUNT(*) FROM ams.building
WHERE parents IS NULL AND feature_id <> id;
----
0

# Reciprocal hierarchy: clear a parent's children array, reconcile restores it
# from the child's parents array.
statement ok
UPDATE ams.building SET children = NULL
  WHERE id = 'NL.IMBAG.Pand.0503100000016459';

statement ok
PRAGMA cityparquet_reconcile('ams');

query T
SELECT children FROM ams.building WHERE id = 'NL.IMBAG.Pand.0503100000016459';
----
[NL.IMBAG.Pand.0503100000016459-0]

# bbox: corrupt it, reconcile recomputes from geometry unioned with descendants.
statement ok
UPDATE ams.building
  SET bbox = {'min_x': 0.0, 'min_y': 0.0, 'min_z': 0.0, 'max_x': 0.0, 'max_y': 0.0, 'max_z': 0.0}
  WHERE id = 'NL.IMBAG.Pand.0503100000005156';

statement ok
PRAGMA cityparquet_reconcile('ams');

query I
SELECT bbox.max_z > 0 FROM ams.building WHERE id = 'NL.IMBAG.Pand.0503100000005156';
----
true

# A parent's bbox contains its child's — the union-across-descendants rule.
query I
SELECT COUNT(*) FROM ams.building p
JOIN ams.building c ON c.id = p.children[1]
WHERE p.bbox.max_z < c.bbox.max_z OR p.bbox.min_z > c.bbox.min_z;
----
0

# After reconcile the package validates clean.
query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_validate('ams')) WHERE severity = 'error';
----
0

# checks := restricts what is re-derived.
statement ok
UPDATE ams.building SET feature_id = 'wrong'
  WHERE id = 'NL.IMBAG.Pand.0503100000012869-0';

statement ok
PRAGMA cityparquet_reconcile('ams', checks := ['bbox']);

query T
SELECT feature_id FROM ams.building WHERE id = 'NL.IMBAG.Pand.0503100000012869-0';
----
wrong

# The _sql twin returns text and mutates nothing.
query I
SELECT cityparquet_reconcile_sql('ams') LIKE '%UPDATE%';
----
true

query T
SELECT feature_id FROM ams.building WHERE id = 'NL.IMBAG.Pand.0503100000012869-0';
----
wrong
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
./build/release/test/unittest --test-dir . "test/sql/cityparquet_reconcile.test"
```

Expected: `Catalog Error` on `cityparquet_reconcile`.

- [ ] **Step 3: Write the header**

Create `src/include/cityjson/cityparquet_reconcile.hpp`:

```cpp
#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! geometry_lod* column names of one table, in catalog order.
std::vector<std::string> GeometryLodColumns(ClientContext &context, const std::string &schema,
                                            const std::string &table);

//! Re-derivation script. `checks` selects from {"feature_id","hierarchy","bbox"};
//! empty means all three. The emitted order is always feature_id, hierarchy, bbox —
//! bbox depends on the corrected hierarchy.
std::string BuildReconcileSQL(ClientContext &context, const std::string &schema,
                              const std::vector<std::string> &checks);

void RegisterCityParquetReconcileFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 4: Write the implementation**

Create `src/cityjson/cityparquet_reconcile.cpp`. The three phases, as generated SQL over the `all_objects` union from Task 3 (factor that CTE builder into `cityparquet_package.cpp` and share it rather than duplicating):

**feature_id** — a recursive CTE walking each object to its root:

```sql
WITH RECURSIVE roots AS (
  SELECT id, id AS root FROM all_objects WHERE parents IS NULL OR len(parents) = 0
  UNION ALL
  SELECT o.id, r.root FROM all_objects o JOIN roots r ON r.id = o.parents[1]
)
UPDATE <schema>.<table> t SET feature_id = r.root FROM roots r WHERE r.id = t.id;
```

Emit one `UPDATE` per object table. `parents[1]` is the spec's root-parent chain; an object with several parents takes the first, matching the reader's existing behaviour.

**hierarchy** — rebuild each row's `children` from the `parents` arrays that point at it, preserving any existing `children_roles` by position where the child set is unchanged, and setting `children_roles` to NULL where it is not (a rebuilt child list has no roles to carry):

```sql
UPDATE <schema>.<table> t
SET children = c.kids,
    children_roles = CASE WHEN t.children IS NOT DISTINCT FROM c.kids THEN t.children_roles ELSE NULL END
FROM (SELECT p AS parent_id, list(id ORDER BY id) AS kids
      FROM all_objects, UNNEST(parents) AS u(p) GROUP BY p) c
WHERE c.parent_id = t.id;
```

Plus a companion `UPDATE … SET children = NULL, children_roles = NULL` for rows no object claims as a parent.

**bbox** — bottom-up, own geometry unioned with descendants, via a recursive CTE. Own extent first:

```sql
CREATE OR REPLACE TEMP TABLE __cp_own_extent AS
SELECT id,
       { 'min_x': least(...), 'min_y': ..., 'max_z': greatest(...) } AS ext
FROM ( SELECT id,
              cityjson_wkb_extent(geometry_lod0_0) AS e0,
              cityjson_wkb_extent(geometry_lod2_2) AS e1  -- one per discovered column
       FROM <schema>.<table> );
```

Discover the `geometry_lod*` columns per table with `GeometryLodColumns` and generate one `cityjson_wkb_extent(<col>)` term each — never hard-code an LoD set. Then propagate upward with a recursive CTE over `parents`, unioning each descendant's own extent into every ancestor, and write the result back per table. A row with neither geometry nor descendants gets `bbox = NULL`.

Register:

```cpp
void RegisterCityParquetReconcileFunctions(ExtensionLoader &loader) {
	auto pragma = PragmaFunction::PragmaCall("cityparquet_reconcile", PragmaReconcile, {LogicalType::VARCHAR});
	pragma.named_parameters["checks"] = LogicalType::LIST(LogicalType::VARCHAR);
	loader.RegisterFunction(pragma);

	ScalarFunction sql_fn("cityparquet_reconcile_sql", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                      ReconcileSQLScalar);
	loader.RegisterFunction(sql_fn);
}
```

- [ ] **Step 5: Wire into build and loader, build, run the test**

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest
./build/release/test/unittest --test-dir . "test/sql/cityparquet_reconcile.test"
```

- [ ] **Step 6: Run the full suite and commit**

```bash
./build/release/test/unittest --test-dir . "test/sql/*"
git add src/include/cityjson/cityparquet_reconcile.hpp src/cityjson/cityparquet_reconcile.cpp \
        src/cityjson_extension.cpp CMakeLists.txt test/sql/cityparquet_reconcile.test
git commit -m "feat(cityparquet): cityparquet_reconcile

Re-derives feature_id, reciprocal hierarchy and bbox after a raw SQL
edit. Emitted order is normative: bbox depends on the corrected
hierarchy, so it must run last."
```

---

### Task 5: `cityparquet_delete`

**Files:**
- Create: `src/include/cityjson/cityparquet_delete.hpp`
- Create: `src/cityjson/cityparquet_delete.cpp`
- Modify: `CMakeLists.txt`, `src/cityjson_extension.cpp`
- Test: `test/sql/cityparquet_delete.test`

**Interfaces:**
- Consumes: `ObjectTablesInSchema`, `QualifiedName` (Task 2); `BuildReconcileSQL` (Task 4) — delete's final phase is a reconcile.
- Produces:
  - `PRAGMA cityparquet_delete(schema VARCHAR, predicate VARCHAR [, cascade := BOOLEAN] [, tables := VARCHAR[]])`
  - scalar `cityparquet_delete_sql(schema VARCHAR, predicate VARCHAR) -> VARCHAR`
  - `std::string BuildDeleteSQL(ClientContext &context, const std::string &schema, const std::string &predicate, bool cascade, const std::vector<std::string> &tables);`

Algorithm, in order (design §Delete):
1. Resolve matched rows. **Predicate scoping:** parse the predicate with `Parser::ParseExpressionList`, walk it for `ColumnRefExpression`, and apply it only to object tables carrying every referenced column. Skip the rest. Without this, a predicate naming `b3_h_dak_max` fails to bind against `transportation`.
2. Grow to the transitive `children` closure — **via `children`, not `feature_id =`**. A predicate may match a non-root object; deleting a `BuildingPart` must not take out the parent `Building` that shares its `feature_id`.
3. `DELETE` from every in-scope table.
4. Strip deleted ids from survivors' `parents` / `children` / `children_roles`, keeping `children_roles` positionally aligned via `list_zip` + `list_filter`.
5. Recompute `feature_id` — required because `cascade := false` orphans descendants whose `feature_id` still names the deleted root.
6. Recompute `bbox` bottom-up.

Steps 5–6 are exactly `BuildReconcileSQL(context, schema, {"feature_id", "bbox"})` — call it, do not reimplement.

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityparquet_delete.test`:

```
# name: test/sql/cityparquet_delete.test
# description: cityparquet_delete — cascade, survivor cleanup, derived state
# group: [sql]

require cityjson

statement ok
CREATE SCHEMA ams;

statement ok
CREATE TABLE ams.building AS
  SELECT * FROM read_cityjsonseq('test/data/delft_subset.city.jsonl');

statement ok
PRAGMA cityparquet_init('ams');

query I
SELECT COUNT(*) FROM ams.building;
----
20

# Deleting a root Building cascades to its BuildingPart child.
statement ok
PRAGMA cityparquet_delete('ams', $$id = 'NL.IMBAG.Pand.0503100000012869'$$);

query I
SELECT COUNT(*) FROM ams.building
WHERE id IN ('NL.IMBAG.Pand.0503100000012869', 'NL.IMBAG.Pand.0503100000012869-0');
----
0

query I
SELECT COUNT(*) FROM ams.building;
----
18

# Cascade must NOT go upward: deleting a BuildingPart leaves its parent alive,
# even though they share a feature_id.
statement ok
PRAGMA cityparquet_delete('ams', $$id = 'NL.IMBAG.Pand.0503100000016459-0'$$);

query I
SELECT COUNT(*) FROM ams.building WHERE id = 'NL.IMBAG.Pand.0503100000016459';
----
1

# The surviving parent no longer references the deleted child.
query T
SELECT children FROM ams.building WHERE id = 'NL.IMBAG.Pand.0503100000016459';
----
NULL

# The package validates clean after every delete.
query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_validate('ams')) WHERE severity = 'error';
----
0

# cascade := false leaves the child alive, and its feature_id is repaired to
# point at itself now that it has become a root.
statement ok
PRAGMA cityparquet_delete('ams', $$id = 'NL.IMBAG.Pand.0503100000005156'$$, cascade := false);

query TT
SELECT id, feature_id FROM ams.building WHERE id = 'NL.IMBAG.Pand.0503100000005156-0';
----
NL.IMBAG.Pand.0503100000005156-0	NL.IMBAG.Pand.0503100000005156-0

query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_validate('ams')) WHERE severity = 'error';
----
0

# A predicate naming an attribute column binds only against tables that have it.
statement ok
CREATE TABLE ams.transportation AS
  SELECT * FROM ams.building WHERE false;

statement ok
PRAGMA cityparquet_init('ams');

statement ok
PRAGMA cityparquet_delete('ams', $$object_type = 'BuildingPart'$$);

query I
SELECT COUNT(*) FROM ams.building WHERE object_type = 'BuildingPart';
----
0

# The whole thing is one transaction: a rollback undoes it entirely.
statement ok
BEGIN;

statement ok
PRAGMA cityparquet_delete('ams', $$object_type = 'Building'$$);

statement ok
ROLLBACK;

query I
SELECT COUNT(*) > 0 FROM ams.building WHERE object_type = 'Building';
----
true

# The _sql twin returns text and mutates nothing.
query I
SELECT cityparquet_delete_sql('ams', $$id = 'x'$$) LIKE '%DELETE FROM%';
----
true
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
./build/release/test/unittest --test-dir . "test/sql/cityparquet_delete.test"
```

Expected: `Catalog Error` on `cityparquet_delete`.

- [ ] **Step 3: Write the header**

Create `src/include/cityjson/cityparquet_delete.hpp`:

```cpp
#pragma once

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

//! Object tables in `schema` against which `predicate` can bind — i.e. those carrying
//! every column the predicate references. Object tables do not share an attribute
//! column set, so applying a predicate to all of them would fail to bind.
std::vector<std::string> TablesBindingPredicate(ClientContext &context, const std::string &schema,
                                                const std::string &predicate,
                                                const std::vector<std::string> &restrict_to);

std::string BuildDeleteSQL(ClientContext &context, const std::string &schema, const std::string &predicate,
                           bool cascade, const std::vector<std::string> &tables);

void RegisterCityParquetDeleteFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb
```

- [ ] **Step 4: Write the implementation**

Create `src/cityjson/cityparquet_delete.cpp`.

`TablesBindingPredicate`: parse with `Parser::ParseExpressionList(predicate)`, recurse each expression via `ParsedExpressionIterator::EnumerateChildren` collecting `ColumnRefExpression::GetColumnName()`, then keep tables whose `TableCatalogEntry::GetColumns()` contains all of them.

The generated script:

```sql
CREATE OR REPLACE TEMP TABLE __cp_deleted AS
WITH RECURSIVE seed AS (
  SELECT id FROM <schema>.<t1> WHERE <predicate>
  UNION ALL
  SELECT id FROM <schema>.<t2> WHERE <predicate>          -- one per binding table
),
closure AS (
  SELECT id FROM seed
  UNION                                                    -- UNION, not UNION ALL: terminates on cycles
  SELECT o.id FROM all_objects o, UNNEST(o.parents) AS u(p)
  JOIN closure c ON c.id = u.p
)
SELECT id FROM closure;                                    -- cascade := true
-- cascade := false: SELECT id FROM seed;

DELETE FROM <schema>.<table> WHERE id IN (SELECT id FROM __cp_deleted);   -- per object table

-- survivor cleanup, per object table, children_roles kept aligned
UPDATE <schema>.<table> t
SET children = list_transform(f.kept, x -> x[1]),
    children_roles = CASE WHEN t.children_roles IS NULL THEN NULL
                          ELSE list_transform(f.kept, x -> x[2]) END
FROM (
  SELECT id, list_filter(list_zip(children, COALESCE(children_roles, [NULL::VARCHAR])),
                         x -> x[1] NOT IN (SELECT id FROM __cp_deleted)) AS kept
  FROM <schema>.<table> WHERE children IS NOT NULL
) f WHERE t.id = f.id;

UPDATE <schema>.<table> t
SET parents = list_filter(parents, x -> x NOT IN (SELECT id FROM __cp_deleted))
WHERE parents IS NOT NULL;

-- normalise emptied lists to NULL so validate's "root" test stays simple
UPDATE <schema>.<table> SET children = NULL, children_roles = NULL WHERE len(children) = 0;
UPDATE <schema>.<table> SET parents = NULL WHERE len(parents) = 0;
```

then append `BuildReconcileSQL(context, schema, {"feature_id", "bbox"})`, and finish with `DROP TABLE __cp_deleted;`.

Note on `list_zip` with a NULL roles array: `list_zip(children, NULL)` yields structs whose second field is NULL, which is why the `CASE` above preserves NULL roles rather than materialising a list of NULLs. Verify this behaviour before relying on it:

```bash
./build/release/duckdb -c "SELECT list_zip(['a','b'], NULL::VARCHAR[]);"
```

Register with named parameters:

```cpp
auto pragma = PragmaFunction::PragmaCall("cityparquet_delete", PragmaDelete,
                                         {LogicalType::VARCHAR, LogicalType::VARCHAR});
pragma.named_parameters["cascade"] = LogicalType::BOOLEAN;
pragma.named_parameters["tables"] = LogicalType::LIST(LogicalType::VARCHAR);
loader.RegisterFunction(pragma);
```

- [ ] **Step 5: Wire into build and loader, build, run the test**

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest
./build/release/test/unittest --test-dir . "test/sql/cityparquet_delete.test"
```

- [ ] **Step 6: Run the full suite and commit**

```bash
./build/release/test/unittest --test-dir . "test/sql/*"
git add src/include/cityjson/cityparquet_delete.hpp src/cityjson/cityparquet_delete.cpp \
        src/cityjson_extension.cpp CMakeLists.txt test/sql/cityparquet_delete.test
git commit -m "feat(cityparquet): cityparquet_delete with cascade

Cascade walks children transitively, never feature_id equality: a
predicate may match a non-root object, and deleting a BuildingPart must
not take out the parent Building sharing its feature_id. Survivors are
stripped of dangling references with children_roles kept aligned, then
feature_id and bbox are re-derived."
```

---

### Task 6: `cityparquet_vacuum`

**Files:**
- Modify: `src/cityjson/cityparquet_validate.cpp` — implement `BuildVacuumSQL` (already declared in the Task 3 header) and register the pragma
- Test: `test/sql/cityparquet_vacuum.test`

**Interfaces:**
- Consumes: `ReferencedIds` and `SidecarTablesInSchema` (Task 3 / Task 2).
- Produces: `PRAGMA cityparquet_vacuum(schema VARCHAR)` and scalar `cityparquet_vacuum_sql(schema VARCHAR) -> VARCHAR`. `BuildVacuumSQL` is already declared in `cityparquet_validate.hpp` by Task 3; this task only supplies the body.

- [ ] **Step 1: Write the failing test**

Create `test/sql/cityparquet_vacuum.test`:

```
# name: test/sql/cityparquet_vacuum.test
# description: cityparquet_vacuum — drop unreferenced sidecar rows
# group: [sql]

require cityjson

statement ok
CREATE SCHEMA ams;

statement ok
CREATE TABLE ams.building AS
  SELECT * FROM read_cityjsonseq('test/data/railway_appearance.city.jsonl');

statement ok
CREATE TABLE ams.materials (id BIGINT, name VARCHAR);

statement ok
INSERT INTO ams.materials VALUES (0, 'referenced-or-not'), (999, 'definitely-orphaned');

statement ok
PRAGMA cityparquet_init('ams');

# The id no material_lod* cell mentions is reported as an orphan.
query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_orphans('ams')) WHERE id = '999';
----
1

statement ok
PRAGMA cityparquet_vacuum('ams');

query I
SELECT COUNT(*) FROM ams.materials WHERE id = 999;
----
0

# Vacuum leaves the package clean and reports nothing further.
query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_orphans('ams'));
----
0

# Vacuum is idempotent.
statement ok
PRAGMA cityparquet_vacuum('ams');

query I
SELECT COUNT(*) FROM (PRAGMA cityparquet_orphans('ams'));
----
0
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
./build/release/test/unittest --test-dir . "test/sql/cityparquet_vacuum.test"
```

- [ ] **Step 3: Implement `BuildVacuumSQL`**

One `DELETE` per sidecar table, anti-joined against the same referenced-id sets `BuildOrphansSQL` computes. Factor the referenced-id CTE builder out of `BuildOrphansSQL` into a shared helper so the two cannot drift apart:

```cpp
std::string BuildVacuumSQL(ClientContext &context, const std::string &schema) {
	auto sidecars = SidecarTablesInSchema(context, schema);
	std::string sql;
	for (const auto &sidecar : sidecars) {
		// NOT IN: vacuum removes the rows nothing references. Getting this
		// inverted deletes exactly the rows that are still in use.
		sql += "DELETE FROM " + QualifiedName(schema, sidecar) + " WHERE id NOT IN (" +
		       ReferencedIds(context, schema, sidecar) + ");\n";
	}
	if (sql.empty()) {
		sql = "SELECT 1 WHERE false;";  // nothing to vacuum; still a valid statement
	}
	return sql;
}
```

Register the pragma and the `_sql` twin alongside the Task 3 registrations.

- [ ] **Step 4: Build, run the test, run the full suite, commit**

```bash
cmake --build build/release --target cityjson_extension cityjson_loadable_extension duckdb unittest
./build/release/test/unittest --test-dir . "test/sql/cityparquet_vacuum.test"
./build/release/test/unittest --test-dir . "test/sql/*"
git add src/include/cityjson/cityparquet_validate.hpp src/cityjson/cityparquet_validate.cpp \
        test/sql/cityparquet_vacuum.test
git commit -m "feat(cityparquet): cityparquet_vacuum

Drops sidecar rows no material_lod*/texture_lod*/template reference
mentions. Shares the referenced-id computation with cityparquet_orphans
so the reporter and the deleter cannot drift apart."
```

---

### Task 7: Documentation

**Files:**
- Modify: `README.md` — new "CityParquet package mutation" section after the existing table-function documentation
- Modify: `CLAUDE.md` and `AGENTS.md` — add the new functions to the registered-functions table, and a note on the pragma-generates-SQL mechanism

**Interfaces:**
- Consumes: every function from Tasks 1–6.
- Produces: no code.

- [ ] **Step 1: Document the functions in README.md**

Add a section covering: the package-is-a-schema model, `__cityparquet`, each of `cityparquet_init` / `_validate` / `_orphans` / `_reconcile` / `_delete` / `_vacuum` with its signature and a worked example, `cityjson_wkb_extent`, and the `_sql()` twins. State plainly that mutation is transactional because the pragma expands into SQL DuckDB runs in the caller's transaction, and that `insert_cityjson` / `cityparquet_merge` are not yet implemented.

- [ ] **Step 2: Update CLAUDE.md and AGENTS.md**

Add rows to the registered-functions table for each new function, and a short subsection explaining that `cityparquet_*` mutators are `PragmaFunction`s returning SQL text (`pragma_query_t`), with the pre-batch expansion caveat: a generator's view of the catalog is the state before *any* statement in the submitted script has run, so destination-dependent decisions must be idempotent or deferred into the generated SQL. Keep the two files in sync, per the repo convention.

- [ ] **Step 3: Commit**

```bash
git add README.md CLAUDE.md AGENTS.md
git commit -m "docs: document the CityParquet mutation functions"
```

---

### Task 8: Flag the two spec divergences upstream

Not code. Both were discovered while building and both need a decision from the spec owner; neither blocks this plan.

**Files:**
- Create: `docs/CITYPARQUET_SPEC_QUESTIONS.md`

- [ ] **Step 1: Write the questions document**

Record, with enough context for someone reading it cold:

1. **`bbox` struct field names.** The specification writes `STRUCT<xmin, ymin, zmin, xmax, ymax, zmax DOUBLE>`; this extension has always emitted `min_x, min_y, min_z, max_x, max_y, max_z` (`src/cityjson/column_types.cpp:127-132`). A CityParquet file written by this extension therefore has a `bbox` column a spec-conformant reader will not recognise. Someone must change: either the spec, or the extension plus every test asserting the current shape. This plan deliberately did not decide it.

2. **Sidecar id types.** `materials.id` and `textures.id` are `BIGINT` while `geometry_templates.id` is `VARCHAR`. The spec already lists this as open and calls it "an inconsistency rather than a decision". It becomes blocking in plan 2: a `BIGINT` id remaps on merge by offsetting, whereas a `VARCHAR` id needs prefixing, which mangles a possibly meaningful identifier.

- [ ] **Step 2: Commit**

```bash
git add docs/CITYPARQUET_SPEC_QUESTIONS.md
git commit -m "docs: record two CityParquet spec divergences found during implementation"
```

---

## Deferred to plan 2

- Appearance normalisation: `read_cityjson(…, appearance := 'sidecar')`, `cityjson_materials`, `cityjson_textures`, `cityjson_geometry_templates`.
- Package I/O: `cityparquet_read`, `cityparquet_write` (including `city`/`geo` footer regeneration and the `metadata.json` STAC Item).
- `cityparquet_merge` and `insert_cityjson` / `insert_cityjsonseq` / `insert_flatcitybuf`.

Plan 2 cannot be written in useful detail until Tasks 1–6 land, because its tasks consume the exact signatures they produce.

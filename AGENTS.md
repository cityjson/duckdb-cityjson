
# C++ Agent Guide

This document provides guidance to coding agents focusing on C++ deliverables when working in the DuckDB CityJSON Extension repository.

## Repository Context

- The loadable extension itself is implemented in C++17 and exposes DuckDB SQL functions. (The only Rust in the tree is the vendored `cjseq` library under `src/external/` — a dependency, not the extension.)
- C++ work in this repository usually targets integration scenarios: embedding the extension in C++ applications, authoring DuckDB C++ tests or utilities, and validating the FFI boundary between the Rust extension and DuckDB's C++ API.
- CityJSON data layout mirrors the CityParquet schema exposed by the extension. Inspect the column set through `DESCRIBE` statements or the SQL tests under `test/sql/` to map values in C++.

## Architecture Overview

YOU SHOULD REFERENCE THE DESIGN_DOC.md FILE FOR THE ARCHITECTURAL OVERVIEW.

### CityParquet package mutation layer

`src/cityjson/cityparquet_*.cpp` implements mutation of a CityParquet package held as a
DuckDB schema. These are **`PragmaFunction`s registered with a `pragma_query_t`**: the
function *returns SQL text*, which DuckDB parses and runs in place of the pragma, inside
the caller's transaction (`duckdb/src/planner/statement_preprocessor.cpp:107-122`).
Atomicity is DuckDB's, not ours — the extension only generates text. Each mutating
pragma has a scalar `*_sql()` twin returning the same text without running it.

Functions: `cityparquet_init`, `cityparquet_validate`, `cityparquet_orphans`,
`cityparquet_vacuum`, `cityparquet_reconcile`, `cityparquet_delete`, plus the scalars
`cityjson_wkb_extent` and `cityjson_appearance_ids`. See `CLAUDE.md` for the full table
and `README.md` for usage.

**Traps worth knowing before adding to this layer:**

- **Pragma expansion happens before execution**, for the *whole* submitted script, so a
  generator's view of the catalog and data is pre-batch. Anything destination-dependent
  must be idempotent or deferred into the generated SQL.
- **A PRAGMA cannot be a subquery** — `FROM (PRAGMA x)` is a parser error. Result-returning
  pragmas materialise a temp table and select from it.
- **PRAGMA named parameters use `=`, not `:=`** (`transform_pragma.cpp:26-33`).
- **No subqueries inside lambda bodies.** Hoist the set into a session variable and use
  `list_contains(getvariable(...), x)`.
- **The `JSON` type and `json_extract` are unavailable** (they live in the `json`
  extension, which this one does not require). JSON is carried as `VARCHAR` and parsed in
  C++ with the vendored nlohmann::json.
- **Two ODR link traps.** Binding a *reference* to a `static constexpr` member emits a
  comdat definition that collides with DuckDB's strong one: write
  `LogicalType(LogicalTypeId::DOUBLE)` rather than `LogicalType::DOUBLE` in
  `emplace_back`, and use the non-templated `Catalog::GetEntry(context,
  CatalogType::TABLE_ENTRY, ...)` rather than `Catalog::GetEntry<TableCatalogEntry>`,
  which ODR-uses `TableCatalogEntry::Name`.
- **`StringUtil::Join` takes `duckdb::vector`**, which `std::vector` does not convert to.
- **`SQLString` is a formatting wrapper, not a quoting function**; use
  `KeywordHelper::WriteQuoted(text, '\'')` and `WriteOptionallyQuoted` for identifiers.

## Build & Tooling

1. Run `make` once to prepare the DuckDB build environment. To make use of cache, try to use `GEN=ninja make` instead.
2. Build debug binaries with `make test_debug`. The loadable module lands in `build/debug/extension/cityjson/`. To use DuckDB with loadable extension, exec `./build/release/duckdb`.
3. For release artifacts run `make`.
4. You only need CMake or other C++ build tools when producing auxiliary C++ binaries/tests.

## Using the Extension from C++

- Include DuckDB's header (`duckdb.hpp`) from the DuckDB submodule or your system installation.
- Load the extension dynamically at runtime; the module requires DuckDB 1.4.1 (matching the bundled submodule).
- Example snippet:

```cpp
#include "duckdb.hpp"

int main() {
    duckdb::DuckDB db;
    duckdb::Connection conn(db);
    conn.Query("LOAD './build/debug/extension/cityjson/cityjson.duckdb_extension';");
    auto result = conn.Query("SELECT * FROM read_cityjson('example.city.json');");
    if (result->HasError()) {
        throw std::runtime_error(result->GetError());
    }
    // Access result->GetValue(row, column) to inspect rows.
    return 0;
}
```

- Remember to start DuckDB with unsigned extensions enabled (`allow_unsigned_extensions=true`) when required.

## Interop Notes

- The Rust extension uses Arrow-style column buffers under the hood. When exchanging data with C++, prefer DuckDB logical types and `Value` helpers instead of manual buffer manipulation.
- Geometry coordinates are stored as `LIST<STRUCT<x DOUBLE, y DOUBLE, z DOUBLE>>` columns. Use the logical type metadata returned by DuckDB (e.g., via `PRAGMA table_info`) when reproducing decode logic in C++.
- Metadata such as transforms and CRS live in JSON columns; use DuckDB's JSON functions from C++ queries rather than custom parsers where possible.

## Testing

- Primary tests live under `test/sql/*.test`. Run them with `make test` or `make test_debug` after C++ changes that impact observable behaviour.
- For C++ unit tests, link against DuckDB's testing utilities (located in the DuckDB submodule). Keep them in `test/cpp/` if you introduce new suites.
- Always verify sample CityJSON files across `read_*` and `write_*` functions to ensure encoding parity with the Rust implementation.

## Performance & Memory Guidance

- Batch operations through SQL queries rather than row-by-row API calls; DuckDB's vectorised execution is far more efficient.
- When marshalling large geometries from C++, avoid unnecessary copies. Use `duckdb::Appender` for bulk inserts into staging tables.
- Be mindful of transform metadata; reapply the same scale/offset semantics as the Rust code when emitting raw coordinates.

## Contribution Workflow

- Mirror the Rust code style for documentation and naming when adding C++ bridging code. Use `clang-format` with DuckDB's style if you add new `.cc`/`.hh` files.
- Keep FFI boundaries minimal. Expose new Rust capabilities through SQL functions first; only add direct C/C++ hooks when unavoidable.
- Document any new SQL surface area in both `README.md` and the relevant `.test` files so Rust and C++ contributors share the same contract.

## References

- DuckDB C++ API: <https://duckdb.org/docs/stable/clients/c/api>
- CityJSON specification: <https://www.cityjson.org/specs/2.0.1/>
- DuckDB headers: `duckdb/src/include/duckdb.hpp`, `duckdb/src/include/duckdb/common/types.hpp`

# Arrow-native geometry encoding — duckdb-cityjson Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Scope note:** second leg of the 3-repo `arrow-native-type` experiment. Design:
`../documents/docs/superpowers/specs/2026-07-25-arrow-native-geometry-design.md` in the
parent workspace repo (read it first). **Schema parity with `cityparquet-rs`'s plan is
the hard constraint here** — the user is implementing that plan in parallel right now
(`cityparquet-rs/docs/superpowers/plans/2026-07-25-arrow-native-geometry-cityparquet-rs.md`);
every type name, field order, and nesting shape below is written to match it exactly, so
a file either repo writes is byte-shape-compatible with what the other reads.

**Important correction discovered while researching this plan**: the design doc's
"match today's VARCHAR-JSON" decision for `geometry_properties` is **superseded**. While
this plan was being written, commit `d334b26` ("feat(cityparquet)!: type
geometry_properties as a STRUCT per the spec") landed directly on this repo's own
`arrow-native-type` branch (not `develop` — confirmed by `git log`/`git blame`, dated the
same moment the branch was created). `read_cityjson`/`read_cityjsonseq` now **already**
emit `geometry_properties_lod*` as `ColumnType::GeometryPropertiesStruct` —
`STRUCT("type" VARCHAR, surfaces JSON, face_semantics INTEGER[], shells INTEGER[][])` —
matching the spec and `cityparquet-rs` exactly. This is good news for this experiment (no
representation mismatch to work around) but it's also a **breaking change to the existing
WKB pipeline**: `duckdb-3d`'s `ST_3DFromWKB(BLOB, VARCHAR)` 2-arg overload can no longer
consume this column directly without an explicit `to_json(...)` cast (a real, if
documented, ergonomics regression — see `duckdb-3d`'s own plan, which fixes this as its
first task, ahead of the new arrow-native work). Nothing in *this* plan needs to touch
`geometry_properties` — it's already exactly what's needed.

**Goal:** add `geometry_encoding := 'wkb' | 'arrow-native'` (default `'wkb'`) to
`read_cityjson`/`read_cityjsonseq`, writing `geometry_lod<M>_<m>` as a 5-level nested
`LIST` (solid → shell → face → ring → vertex-pool index) with a sibling
`geometry_vertices_lod<M>_<m>` (`LIST<STRUCT<x,y,z DOUBLE>>`) instead of the WKB `BLOB`,
when `arrow-native` is chosen. `geometry_properties_lod*` is unchanged either way.

**Architecture:** A new `ArrowNativeEncoder` (mirrors `WKBEncoder`'s static-method shape)
walks a `Geometry`'s `boundaries` JSON exactly like `WKBEncoder`/`GeometryPropertiesSerializer`
already do, producing a `CompactedGeometry` (vertices + nested solid/shell/face/ring index
structure) via **distinct-source-index compaction** — never coordinate-value dedup, same
invariant as the Rust plan. New `WriteGeometryArrowNative`/`WriteGeometryVertices`
functions populate the DuckDB `Vector`s directly, following the exact
`ListVector::Reserve`/`GetEntry`/`SetListSize` idiom this file's existing
`AppendIntList`/`AppendIntListList` already use for `shells`/`face_semantics` — extended
three more nesting levels and to a `Struct` leaf.

## Global Constraints

- **Strict red-green TDD**, unit test first (`test/cpp/` if this repo has one — check;
  otherwise this repo's convention is SQL-level tests in `test/sql/`, per the existing
  `.test` file layout observed — confirm which applies before Task 1's first test).
- **Real fixtures only** — this repo's `test/sql/*.test` files load real CityJSON
  fixtures (grep an existing `.test` file, e.g. `st_3d_hollow_solid.test`'s CityJSON
  interop counterpart `cityjson_hollow_solid.test`, for the fixture-loading convention
  before writing new tests).
- British English in prose/comments, matching this repo's existing style.
- Run `codex exec -m gpt-5.6-sol -s read-only` review at the end (matches
  `cityparquet-rs`'s own convention and the user's explicit request for this reviewer
  across the whole experiment).
- Commit after every task; push to `origin/arrow-native-type` after each milestone.
- **Phase-1 type scope**: `MultiSurface`, `CompositeSurface`, `Solid`, `MultiSolid`,
  `CompositeSolid` only (matches the Rust plan exactly — `Point`/`MultiPoint`/
  `MultiLineString`/`GeometryInstance` are out of scope for the new encoding).
- **Schema parity is non-negotiable**: field names, nesting order, and padding-dimension
  convention below MUST match `cityparquet-rs`'s
  `arrow_native_geometry_data_type()`/`arrow_native_vertices_data_type()` exactly (see
  that plan's Task 1) — this is what makes the cross-repo consistency gate in the design
  doc's testing plan meaningful at all.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/include/cityjson/types.hpp` | Modify: `ColumnType` gains `GeometryArrowNative`, `GeometryVerticesArrowNative`. |
| `src/cityjson/column_types.cpp` | Modify: `ToString`/`ToLogicalTypeId`/`ToDuckDBType` gain cases for the two new types. |
| `src/include/cityjson/arrow_native_encoder.hpp`, `src/cityjson/arrow_native_encoder.cpp` | Create: `ArrowNativeEncoder` — `Geometry` + vertex pool → `CompactedGeometry` (distinct-index compaction), mirroring `WKBEncoder`. |
| `src/include/cityjson/vector_writer.hpp`, `src/cityjson/vector_writer.cpp` | Modify: add `WriteGeometryArrowNative`/`WriteGeometryVertices`, following the existing `AppendIntList`/`AppendIntListList` idiom. |
| `src/include/cityjson/city_object_utils.hpp`, `src/cityjson/city_object_utils.cpp` | Modify: add `GetGeometryArrowNative`, mirroring `GetGeometryWKB`. |
| `src/cityjson/lod_table.cpp` | Modify: `LODTableUtils::GetGeometryColumns` becomes encoding-aware, adding the vertices sibling column only for `arrow-native`. |
| `src/cityjson/scan_function.cpp` | Modify: `WriteCityObjectRow` gains a case for the two new `ColumnType`s. |
| `src/include/cityjson/table_function.hpp`, `src/cityjson/bind_function.cpp` | Modify: `CityJSONReadOptions`/`CityJSONBindData` gain `geometry_encoding`; `ParseCityJSONReadOptions` parses the new named parameter. |
| `src/cityjson/geoparquet_table_function.cpp` | Modify: `GeoParquetTypeName`-equivalent logic becomes encoding-aware (design doc round-2 finding). |
| `test/sql/arrow_native_geometry.test` (or `test/cpp/test_arrow_native_encoder.cpp` if this repo has a `test/cpp/` — verify in Task 1) | Create: the round-trip/shape tests. |

---

### Task 0 (was Task 8): `COPY ... TO (FORMAT PARQUET)` deep-nesting spike — **RUN, PASSED**

Run first, as this document's Task 8 itself recommends, using a synthetic
hand-built value in pure SQL — no extension code needed, so it gated the work
before any was invested.

- **Round-trip: PASS.** A hand-built `INTEGER[][][][][]` plus
  `STRUCT(x,y,z)[]` written with `COPY ... TO (FORMAT PARQUET)` and read back
  compares equal (`geom_equal`/`verts_equal` both true) with `typeof()`
  unchanged on both columns. DuckDB does not collapse or mangle at this depth.
- **Physical field names: PASS.** `parquet_schema()` shows the standard
  three-level LIST annotation (`list` REPEATED → `element`) at all five levels,
  and `x`/`y`/`z` survive by name under the vertex-pool list.
- **Cross-producer naming difference, recorded, non-blocking.**
  `cityparquet-rs` names every list child `"item"` and marks inner elements
  non-nullable; DuckDB writes `"element"`/OPTIONAL. A Parquet file written with
  arrow's naming *and* REQUIRED inner elements reads back in DuckDB as exactly
  `INTEGER[][][][][]` / `STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)[]` with values
  intact — Parquet identifies a LIST's element positionally (the repeated
  group's single child), not by name, so the difference is cosmetic in the
  physical schema. **Still to do:** repeat against a file written by
  `cityparquet-rs` itself rather than a stand-in, once that branch pushes a
  fixture — the check above used a hand-built file, since the local
  `cityparquet` binary predates `--geometry-encoding`.

---

### Task 1: Confirm test placement convention, then `ColumnType` additions

**Files:**
- Modify: `src/include/cityjson/types.hpp`
- Modify: `src/cityjson/column_types.cpp`
- Test: TBD by Step 1's finding

**Interfaces:**
- Produces: `ColumnType::GeometryArrowNative` (→ 5-level nested `LIST<...<LIST<INTEGER>>...>`), `ColumnType::GeometryVerticesArrowNative` (→ `LIST<STRUCT<x DOUBLE, y DOUBLE, z DOUBLE>>`).

- [x] **Step 1: Determine this repo's unit-test convention before writing anything**

Run: `find test -maxdepth 1 -type d` and `ls test/cpp 2>&1`. This repo's directory
listing (gathered while writing this plan) showed no `test/cpp/` — only `test/sql/*.test`
— unlike `duckdb-3d`, which has both. **Confirm this directly** rather than trust this
plan's note: if there genuinely is no C++ unit-test harness, every "write a failing
test" step below for pure-C++ logic (the encoder, the compaction algorithm) needs to
become a `test/sql/` test exercising it through `read_cityjson(...)` SQL instead — slower
red-green cycles, but this repo's actual convention, not invented. If a `test/cpp/`
does exist (double-check — a build artifact or CI config might reference one this plan's
research missed), use it for Task 2's pure-logic tests and reserve `test/sql/` for
integration.

- [x] **Step 2: Write the failing test**

If `test/cpp/` exists, add a small compile-time/type test (mirroring whatever pattern
`test_metadata.cpp` uses for the `catch.hpp` framework observed in this plan's research).
If SQL-only, add to a new `test/sql/arrow_native_geometry.test`:

```sql
# name: test/sql/arrow_native_geometry.test
# description: arrow-native geometry encoding column types
# group: [cityjson]

require cityjson

statement ok
CREATE TABLE t AS SELECT * FROM read_cityjson('test/fixtures/PLACEHOLDER.city.json', geometry_encoding := 'arrow-native');

query I
SELECT typeof(geometry_lod2_2) FROM t LIMIT 0;
----
INTEGER[][][][][]

query I
SELECT typeof(geometry_vertices_lod2_2) FROM t LIMIT 0;
----
STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)[]
```

(Confirm the real fixture path and a real LoD suffix present in it against this repo's
`test/fixtures/` directory — grep an existing passing `.test` file for the naming
convention rather than guessing `PLACEHOLDER.city.json`.)

- [x] **Step 3: Run test to verify it fails**

Expected: FAIL — `geometry_encoding` isn't a recognised named parameter yet (binder error), or `ColumnType::GeometryArrowNative` doesn't exist (compile error) if using a C++ test.

- [x] **Step 4: Add the two `ColumnType` values**

In `src/include/cityjson/types.hpp`, extend the `enum class ColumnType` (confirmed
current members end with `AppearanceJson`):

```cpp
	AppearanceJson,           // JSON - per-LoD material_lod*/texture_lod* appearance (§11)

	// Arrow-native geometry encoding (experimental, arrow-native-type branch — see
	// docs/superpowers/specs/2026-07-25-arrow-native-geometry-design.md in the parent
	// workspace repo): a nested LIST replaces the WKB BLOB, paired with a vertex-pool
	// sibling column. Schema shape MUST match cityparquet-rs's
	// arrow_native_geometry_data_type()/arrow_native_vertices_data_type() exactly.
	GeometryArrowNative,          // solid -> shell -> face -> ring -> INTEGER (vertex-pool index)
	GeometryVerticesArrowNative,  // LIST<STRUCT<x DOUBLE, y DOUBLE, z DOUBLE>>
};
```

- [x] **Step 5: Add the `DataType` construction to `column_types.cpp`**

In `src/cityjson/column_types.cpp`, add cases to all three `ColumnTypeUtils` methods (confirmed real cases in this plan's research: `ToString` around line ~41, `ToLogicalTypeId` around ~77, `ToDuckDBType` around ~139):

```cpp
// ToString:
case ColumnType::GeometryArrowNative:
	return "INTEGER[][][][][]";
case ColumnType::GeometryVerticesArrowNative:
	return "STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)[]";
```

```cpp
// ToLogicalTypeId — both are composite, no single LogicalTypeId fits; follow this
// file's existing convention for other composite types (grep how GeometryPropertiesStruct's
// ToLogicalTypeId case is written — confirmed at line ~77 to return LogicalTypeId::STRUCT
// even though it's used only informationally, since ToDuckDBType is what's load-bearing):
case ColumnType::GeometryArrowNative:
	return LogicalTypeId::LIST;
case ColumnType::GeometryVerticesArrowNative:
	return LogicalTypeId::LIST;
```

```cpp
// ToDuckDBType — the real, load-bearing construction:
case ColumnType::GeometryArrowNative: {
	// solid -> shell -> face -> ring -> index (5 LIST levels), matching
	// cityparquet-rs's arrow_native_geometry_data_type() exactly.
	auto ring = LogicalType::LIST(LogicalType::INTEGER);
	auto face = LogicalType::LIST(ring);
	auto shell = LogicalType::LIST(face);
	auto solid = LogicalType::LIST(shell);
	return LogicalType::LIST(solid);
}
case ColumnType::GeometryVerticesArrowNative: {
	child_list_t<LogicalType> coord_fields;
	coord_fields.push_back(make_pair("x", LogicalType::DOUBLE));
	coord_fields.push_back(make_pair("y", LogicalType::DOUBLE));
	coord_fields.push_back(make_pair("z", LogicalType::DOUBLE));
	return LogicalType::LIST(LogicalType::STRUCT(std::move(coord_fields)));
}
```

(Confirm `child_list_t<LogicalType>`/`make_pair` is the exact idiom this file already
uses elsewhere for STRUCT construction — grep `LogicalType::STRUCT(` in this same file
for the established pattern rather than trusting this snippet verbatim; adjust to match.)

- [x] **Step 6: Run test to verify it passes**

Expected: still FAILS at the `geometry_encoding := 'arrow-native'` binder step — Task 1
only defines the types, nothing wires the named parameter or writer yet (Tasks 2-5 do
that). Note the exact failure (should now be "unrecognized named parameter
geometry_encoding", not a `ColumnType` compile error) and move to Task 2.

- [x] **Step 7: Commit**

```bash
git add src/include/cityjson/types.hpp src/cityjson/column_types.cpp test/
git commit -m "feat(schema): ColumnType::GeometryArrowNative/GeometryVerticesArrowNative"
```

---

### Task 2: `geometry_encoding` named parameter

**Files:**
- Modify: `src/include/cityjson/table_function.hpp`
- Modify: `src/cityjson/bind_function.cpp`

**Interfaces:**
- Produces: `CityJSONReadOptions.geometry_encoding` / `CityJSONBindData.geometry_encoding` (`enum class GeometryEncoding { Wkb, ArrowNative }`, default `Wkb`), parsed from `geometry_encoding := 'wkb' | 'arrow-native'`.

- [ ] **Step 1: Write the failing test**

```sql
statement error
SELECT * FROM read_cityjson('test/fixtures/PLACEHOLDER.city.json', geometry_encoding := 'not-a-real-encoding') LIMIT 0;
----
geometry_encoding must be "wkb" or "arrow-native"
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — no such named parameter recognised, so the error message/behaviour doesn't match yet (silently ignored today, per `ParseCityJSONReadOptions`'s current `if/else if` chain having no `geometry_encoding` branch).

- [ ] **Step 3: Add the enum and threading**

In `src/include/cityjson/table_function.hpp`, add near `CityJSONReadOptions`:

```cpp
enum class GeometryEncoding { Wkb, ArrowNative };
```

Add `GeometryEncoding geometry_encoding = GeometryEncoding::Wkb;` to both
`CityJSONReadOptions` (line ~47-51) and `CityJSONBindData` (line ~27-42), next to the
existing `use_wkb_encoding`/`target_lod` fields.

In `src/cityjson/bind_function.cpp`'s `ParseCityJSONReadOptions` (confirmed structure at
lines 15-30), add a branch to the existing `for (auto &kv : input.named_parameters)`
loop:

```cpp
} else if (kv.first == "geometry_encoding") {
	auto value = StringValue::Get(kv.second);
	if (value == "wkb") {
		options.geometry_encoding = GeometryEncoding::Wkb;
	} else if (value == "arrow-native") {
		options.geometry_encoding = GeometryEncoding::ArrowNative;
	} else {
		throw BinderException(function_name + ": geometry_encoding must be \"wkb\" or \"arrow-native\", got \"" + value + "\"");
	}
}
```

Thread `options.geometry_encoding` into `bind_data.geometry_encoding` wherever the bind
function constructs `CityJSONBindData` from `CityJSONReadOptions` (grep the bind
function's constructor call — likely right after `ParseCityJSONReadOptions` returns).
Also register `"geometry_encoding"` as a valid named parameter with the table function's
`named_parameters` map in whichever file declares `read_cityjson`'s/`read_cityjsonseq`'s
`TableFunction` object (grep `named_parameters["lod"]` to find every registration site —
there should be one per table function variant, e.g. `read_cityjson`, `read_cityjsonseq`,
possibly not `read_flatcitybuf` since arrow-native is CityJSON-model-specific).

- [ ] **Step 4: Run test to verify it passes**

Expected: PASS (the error-message test). Also add and run a positive-path smoke test
(`geometry_encoding := 'arrow-native'` binds without error, even though it doesn't yet
write real data — Task 5 makes it actually populate correctly).

- [ ] **Step 5: Commit**

```bash
git add src/include/cityjson/table_function.hpp src/cityjson/bind_function.cpp test/
git commit -m "feat(bind): geometry_encoding named parameter (wkb default, arrow-native opt-in)"
```

---

### Task 3: `LODTableUtils::GetGeometryColumns` becomes encoding-aware

**Files:**
- Modify: `src/cityjson/lod_table.cpp`

**Interfaces:**
- Consumes: `GeometryEncoding` (Task 2).
- Produces: `LODTableUtils::GetGeometryColumns(const std::string &lod, GeometryEncoding encoding)` (signature change — was `(const std::string &lod)`).

- [ ] **Step 1: Write the failing test**

```sql
statement ok
CREATE TABLE t2 AS SELECT * FROM read_cityjson('test/fixtures/PLACEHOLDER.city.json', geometry_encoding := 'arrow-native') LIMIT 0;

query I
SELECT column_name FROM (DESCRIBE t2) WHERE column_name LIKE 'geometry_vertices%';
----
geometry_vertices_lod2_2

statement ok
CREATE TABLE t3 AS SELECT * FROM read_cityjson('test/fixtures/PLACEHOLDER.city.json') LIMIT 0;  -- default wkb

query I
SELECT count(*) FROM (DESCRIBE t3) WHERE column_name LIKE 'geometry_vertices%';
----
0
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — no `geometry_vertices_lod*` column exists yet regardless of encoding (compile error once `GetGeometryColumns`'s signature changes are attempted, or a missing-column result before that).

- [ ] **Step 3: Update `GetGeometryColumns`**

In `src/cityjson/lod_table.cpp` (confirmed body at lines 101-116):

```cpp
std::vector<Column> LODTableUtils::GetGeometryColumns(const std::string &lod, GeometryEncoding encoding) {
	std::string suffix = FormatLODAsColumnSuffix(lod);
	std::vector<Column> columns = {
	    Column("geometry_" + suffix,
	           encoding == GeometryEncoding::Wkb ? ColumnType::GeometryWKB : ColumnType::GeometryArrowNative),
	};
	if (encoding == GeometryEncoding::ArrowNative) {
		columns.push_back(Column("geometry_vertices_" + suffix, ColumnType::GeometryVerticesArrowNative));
	}
	columns.push_back(Column("geometry_properties_" + suffix, ColumnType::GeometryPropertiesStruct));
	columns.push_back(Column("material_" + suffix, ColumnType::AppearanceJson));
	columns.push_back(Column("texture_" + suffix, ColumnType::AppearanceJson));
	columns.push_back(Column("bbox", ColumnType::GeographicalExtent));
	return columns;
}
```

Update the header declaration in `src/include/cityjson/lod_table.hpp` to match. Update
every call site (grep `GetGeometryColumns(` — likely one in the wide-layout schema
builder and one in the `lod =>` single-LoD path per this plan's earlier research) to pass
`bind_data.geometry_encoding`/`options.geometry_encoding` from whichever scope has it.

- [ ] **Step 4: Run test to verify it passes**

Expected: PASS. Run the full existing SQL test suite (`make test` or this repo's real
test-running command — check `justfile`/`Makefile`) to confirm no regression to the WKB
path (default) — every existing test uses no `geometry_encoding` parameter, so must be
completely unaffected.

- [ ] **Step 5: Commit**

```bash
git add src/cityjson/lod_table.cpp src/include/cityjson/lod_table.hpp
git commit -m "feat(schema): geometry_vertices_lod* sibling column, arrow-native only"
```

---

### Task 4: `ArrowNativeEncoder` — the compaction algorithm

**Files:**
- Create: `src/include/cityjson/arrow_native_encoder.hpp`, `src/cityjson/arrow_native_encoder.cpp`

**Interfaces:**
- Consumes: `Geometry`, the feature's vertex pool (`const std::vector<std::array<double,3>>&`), `const std::optional<Transform>&` — same three arguments `WKBEncoder::Encode` already takes.
- Produces:
  ```cpp
  struct CompactedFace { std::vector<std::vector<uint32_t>> rings; }; // ring -> vertex-pool index
  struct CompactedShell { std::vector<CompactedFace> faces; };
  struct CompactedSolid { std::vector<CompactedShell> shells; };
  struct CompactedGeometry {
      std::vector<std::array<double, 3>> vertices; // distinct-source-index-compacted pool
      std::vector<CompactedSolid> solids;           // padded to length 1 for surface types
  };
  class ArrowNativeEncoder {
  public:
      static CompactedGeometry Encode(const Geometry &geometry,
                                       const std::vector<std::array<double, 3>> &vertices,
                                       const std::optional<Transform> &transform = std::nullopt);
  };
  ```

- [ ] **Step 1: Write the failing test**

If this repo has `test/cpp/` (Task 1's finding), add there; otherwise write this as a
temporary standalone test binary/scratch harness during development and translate the
assertions into a `test/sql/` round-trip test in Task 5 — pure-algorithm testing without
a C++ harness is awkward but the algorithm itself should still be developed and verified
in isolation before wiring it into DuckDB `Vector`s, per this plan's TDD discipline.

```cpp
TEST_CASE("ArrowNativeEncoder compacts shared MultiSurface vertices", "[arrow_native_encoder]") {
	std::vector<std::array<double, 3>> vertices = {
	    {0, 0, 0}, {1, 0, 0}, {1, 1, 0}, {0, 1, 0}
	};
	Geometry geom;
	geom.type = "MultiSurface";
	geom.boundaries = json::parse(R"([[[0,1,2]],[[0,2,3]]])");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);
	REQUIRE(compacted.vertices.size() == 4); // shared indices 0 and 2 compacted, not duplicated
	REQUIRE(compacted.solids.size() == 1);    // padding dimension
	REQUIRE(compacted.solids[0].shells.size() == 1); // padding dimension
	REQUIRE(compacted.solids[0].shells[0].faces.size() == 2);
	REQUIRE(compacted.solids[0].shells[0].faces[0].rings[0] == std::vector<uint32_t>{0, 1, 2});
	REQUIRE(compacted.solids[0].shells[0].faces[1].rings[0] == std::vector<uint32_t>{0, 2, 3});
}

TEST_CASE("ArrowNativeEncoder never merges distinct indices with equal coordinates", "[arrow_native_encoder]") {
	std::vector<std::array<double, 3>> vertices = {
	    {0, 0, 0}, {0, 0, 0}, {1, 0, 0} // indices 0 and 1: same coordinate, different indices
	};
	Geometry geom;
	geom.type = "MultiSurface";
	geom.boundaries = json::parse(R"([[[0,1,2]]])");

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);
	REQUIRE(compacted.vertices.size() == 3); // NOT 2 — index-identity compaction, not coordinate dedup
}
```

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `ArrowNativeEncoder` doesn't exist.

- [ ] **Step 3: Implement, reusing `WKBEncoder`'s structural helpers where possible**

Check first whether `WKBEncoder`'s private `ApplyTransform`/`GetVertex`/`ReverseRing`
helpers (confirmed present, `wkb_encoder.hpp` private section) are reusable as-is (they're
currently `private` — would need to become `protected`/free functions in a shared header,
or simply reimplemented, since `CompactedGeometry` doesn't need WKB's ring-reversal at
all — **arrow-native rings keep CityJSON's original winding**, since there's no OGC
CCW-exterior convention to satisfy; only the transform/dereference logic
(`ApplyTransform`/`GetVertex`) is genuinely shared). Prefer duplicating the small
transform/dereference helpers over widening `WKBEncoder`'s private surface, since ring
winding is a genuine behavioural difference, not shared logic — a widened `WKBEncoder`
would invite someone to accidentally reuse `ReverseRing` where it doesn't belong.

```cpp
// arrow_native_encoder.cpp
namespace duckdb {
namespace cityjson {

namespace {

std::array<double, 3> ApplyTransform(const std::array<double, 3> &v, const std::optional<Transform> &transform) {
	if (!transform.has_value()) {
		return v;
	}
	return {v[0] * transform->scale[0] + transform->translate[0],
	        v[1] * transform->scale[1] + transform->translate[1],
	        v[2] * transform->scale[2] + transform->translate[2]};
}

//! Distinct-source-index vertex-pool compactor (design doc: index-identity
//! compaction, NEVER coordinate-value dedup — two different source indices
//! with identical coordinates stay two separate pool entries).
class Compactor {
public:
	Compactor(const std::vector<std::array<double, 3>> &vertices, const std::optional<Transform> &transform)
	    : vertices_(vertices), transform_(transform) {
	}

	uint32_t LocalIndex(uint32_t raw) {
		auto it = seen_.find(raw);
		if (it != seen_.end()) {
			return it->second;
		}
		uint32_t local = static_cast<uint32_t>(compacted_.size());
		compacted_.push_back(ApplyTransform(vertices_.at(raw), transform_));
		seen_[raw] = local;
		return local;
	}

	std::vector<uint32_t> Ring(const json &ring_json) {
		std::vector<uint32_t> out;
		out.reserve(ring_json.size());
		for (const auto &idx : ring_json) {
			out.push_back(LocalIndex(idx.get<uint32_t>()));
		}
		return out;
	}

	CompactedFace Face(const json &surface_json) {
		CompactedFace face;
		for (const auto &ring_json : surface_json) {
			face.rings.push_back(Ring(ring_json));
		}
		return face;
	}

	std::vector<std::array<double, 3>> Take() {
		return std::move(compacted_);
	}

private:
	const std::vector<std::array<double, 3>> &vertices_;
	const std::optional<Transform> &transform_;
	std::unordered_map<uint32_t, uint32_t> seen_;
	std::vector<std::array<double, 3>> compacted_;
};

} // namespace

CompactedGeometry ArrowNativeEncoder::Encode(const Geometry &geometry,
                                             const std::vector<std::array<double, 3>> &vertices,
                                             const std::optional<Transform> &transform) {
	Compactor compactor(vertices, transform);
	CompactedGeometry result;

	if (geometry.type == "MultiSurface" || geometry.type == "CompositeSurface") {
		CompactedShell shell;
		for (const auto &surface_json : geometry.boundaries) {
			shell.faces.push_back(compactor.Face(surface_json));
		}
		CompactedSolid solid;
		solid.shells.push_back(std::move(shell)); // padding dimension
		result.solids.push_back(std::move(solid)); // padding dimension
	} else if (geometry.type == "Solid") {
		CompactedSolid solid;
		for (const auto &shell_json : geometry.boundaries) {
			CompactedShell shell;
			for (const auto &surface_json : shell_json) {
				shell.faces.push_back(compactor.Face(surface_json));
			}
			solid.shells.push_back(std::move(shell));
		}
		result.solids.push_back(std::move(solid));
	} else if (geometry.type == "MultiSolid" || geometry.type == "CompositeSolid") {
		for (const auto &solid_json : geometry.boundaries) {
			CompactedSolid solid;
			for (const auto &shell_json : solid_json) {
				CompactedShell shell;
				for (const auto &surface_json : shell_json) {
					shell.faces.push_back(compactor.Face(surface_json));
				}
				solid.shells.push_back(std::move(shell));
			}
			result.solids.push_back(std::move(solid));
		}
	} else {
		throw CityJSONError::Other(
		    "ArrowNativeEncoder: " + geometry.type +
		    " is not supported by the arrow-native encoding in phase 1 (design doc \"Type coverage (v1)\")");
	}

	result.vertices = compactor.Take();
	return result;
}

} // namespace cityjson
} // namespace duckdb
```

Confirm `Geometry::boundaries`'s real type (`json` per `nlohmann`-style usage elsewhere in
this codebase, per `WKBEncoder`'s own `const json &boundaries` parameters) matches the
range-based-for iteration used above; adjust if this repo's `Geometry` struct nests
boundaries differently than assumed (verify against `cityjson_types.hpp`'s real
`Geometry` struct before trusting this snippet verbatim — this plan's research read
`wkb_encoder.hpp`'s comments describing the boundary shapes per type but not
`cityjson_types.hpp`'s `Geometry` struct itself in full).

- [ ] **Step 4: Run tests to verify they pass**

Expected: PASS for both Step-1 tests.

- [ ] **Step 5: Add a `Solid`-with-two-shells test (parity with the Rust plan's equivalent test)**

```cpp
TEST_CASE("ArrowNativeEncoder preserves shell structure for a two-shell Solid", "[arrow_native_encoder]") {
	std::vector<std::array<double, 3>> vertices = {
	    {0,0,0}, {1,0,0}, {0,1,0}, {0,0,1}, {1,0,1}, {0,1,1}
	};
	Geometry geom;
	geom.type = "Solid";
	geom.boundaries = json::parse(R"([ [[[0,1,2]]], [[[3,4,5]]] ])"); // 2 shells, 1 face each

	auto compacted = ArrowNativeEncoder::Encode(geom, vertices, std::nullopt);
	REQUIRE(compacted.vertices.size() == 6);
	REQUIRE(compacted.solids.size() == 1);
	REQUIRE(compacted.solids[0].shells.size() == 2); // UNLIKE the WKB path, arrow-native keeps shell structure IN the geometry column itself, not just geometry_properties.shells
}
```

Run — expect PASS without further implementation change; if it fails, fix the `Solid`
branch (real bug, not an expected-fail step).

**Note on a real design nuance surfaced by this test**: unlike WKB (which flattens all
shells into one flat face list, recovering structure only via
`geometry_properties.shells`), the arrow-native physical shape's shell dimension is
**not** padding for a real `Solid` with multiple shells — it carries real structure. This
is consistent with the design doc (the padding-dimension rule applies only to surface
types, which have no shell concept at all) but worth flagging explicitly here since it's
easy to conflate "padding" with "always length 1".

- [ ] **Step 6: Commit**

```bash
git add src/include/cityjson/arrow_native_encoder.hpp src/cityjson/arrow_native_encoder.cpp test/
git commit -m "feat(arrow-geom): ArrowNativeEncoder — distinct-index vertex pool compaction"
```

---

### Task 5: `WriteGeometryArrowNative`/`WriteGeometryVertices` — DuckDB Vector writers

**Files:**
- Modify: `src/include/cityjson/vector_writer.hpp`, `src/cityjson/vector_writer.cpp`

**Interfaces:**
- Consumes: `CompactedGeometry` (Task 4).
- Produces: `void WriteGeometryArrowNative(Vector *list_vec, const CompactedGeometry &geom, size_t row)`, `void WriteGeometryVertices(Vector *list_vec, const CompactedGeometry &geom, size_t row)`.

- [ ] **Step 1: Write the failing test**

Given this repo's DuckDB `Vector` types need a running DuckDB context to construct
meaningfully, this is most naturally a `test/sql/` test (Task 1's likely finding). Write
it as part of Task 6's end-to-end test instead of in isolation — a bare `Vector` unit
test for a 5-level nested LIST without a real DataChunk/scan context is unusually awkward
plumbing for limited standalone value. Skip ahead to Step 3's implementation, then verify
via Task 6's SQL-level round-trip test (this is a deliberate, justified deviation from
strict one-test-per-function granularity — note it, don't silently skip TDD elsewhere in
this plan).

- [ ] **Step 2: (see Step 1 — no isolated test for this task)**

- [ ] **Step 3: Implement the writers, extending the existing `AppendIntList`/`AppendIntListList` idiom**

Add to `src/cityjson/vector_writer.cpp`, next to the existing `AppendIntList`/`AppendIntListList` (confirmed real code at lines ~377-420):

```cpp
// Append one LIST<INTEGER> ring from a std::vector<uint32_t> of vertex-pool indices.
static void AppendIndexRing(Vector &ring_vec, const std::vector<uint32_t> &ring, size_t row) {
	auto list_size = ListVector::GetListSize(ring_vec);
	FlatVector::GetData<list_entry_t>(ring_vec)[row] = list_entry_t(list_size, ring.size());
	ListVector::Reserve(ring_vec, list_size + ring.size());
	auto &child = ListVector::GetEntry(ring_vec);
	auto child_data = FlatVector::GetData<int32_t>(child);
	for (size_t i = 0; i < ring.size(); i++) {
		child_data[list_size + i] = static_cast<int32_t>(ring[i]);
	}
	ListVector::SetListSize(ring_vec, list_size + ring.size());
}

// Append one LIST<LIST<INTEGER>> face (rings) from a CompactedFace.
static void AppendFace(Vector &face_vec, const CompactedFace &face, size_t row) {
	auto list_size = ListVector::GetListSize(face_vec);
	FlatVector::GetData<list_entry_t>(face_vec)[row] = list_entry_t(list_size, face.rings.size());
	ListVector::Reserve(face_vec, list_size + face.rings.size());
	auto &ring_vec = ListVector::GetEntry(face_vec); // LIST<INTEGER>
	for (size_t i = 0; i < face.rings.size(); i++) {
		AppendIndexRing(ring_vec, face.rings[i], list_size + i);
	}
	ListVector::SetListSize(face_vec, list_size + face.rings.size());
}

// Append one LIST<LIST<LIST<INTEGER>>> shell (faces) from a CompactedShell.
static void AppendShell(Vector &shell_vec, const CompactedShell &shell, size_t row) {
	auto list_size = ListVector::GetListSize(shell_vec);
	FlatVector::GetData<list_entry_t>(shell_vec)[row] = list_entry_t(list_size, shell.faces.size());
	ListVector::Reserve(shell_vec, list_size + shell.faces.size());
	auto &face_vec = ListVector::GetEntry(shell_vec); // LIST<LIST<INTEGER>>
	for (size_t i = 0; i < shell.faces.size(); i++) {
		AppendFace(face_vec, shell.faces[i], list_size + i);
	}
	ListVector::SetListSize(shell_vec, list_size + shell.faces.size());
}

// Append one LIST<LIST<LIST<LIST<INTEGER>>>> solid (shells) from a CompactedSolid.
static void AppendSolid(Vector &solid_vec, const CompactedSolid &solid, size_t row) {
	auto list_size = ListVector::GetListSize(solid_vec);
	FlatVector::GetData<list_entry_t>(solid_vec)[row] = list_entry_t(list_size, solid.shells.size());
	ListVector::Reserve(solid_vec, list_size + solid.shells.size());
	auto &shell_vec = ListVector::GetEntry(solid_vec); // LIST<LIST<LIST<INTEGER>>>
	for (size_t i = 0; i < solid.shells.size(); i++) {
		AppendShell(shell_vec, solid.shells[i], list_size + i);
	}
	ListVector::SetListSize(solid_vec, list_size + solid.shells.size());
}

void WriteGeometryArrowNative(Vector *list_vec, const CompactedGeometry &geom, size_t row) {
	if (geom.solids.empty()) {
		FlatVector::SetNull(*list_vec, row, true);
		return;
	}
	auto list_size = ListVector::GetListSize(*list_vec);
	FlatVector::GetData<list_entry_t>(*list_vec)[row] = list_entry_t(list_size, geom.solids.size());
	ListVector::Reserve(*list_vec, list_size + geom.solids.size());
	auto &solid_vec = ListVector::GetEntry(*list_vec); // LIST<LIST<LIST<LIST<INTEGER>>>>
	for (size_t i = 0; i < geom.solids.size(); i++) {
		AppendSolid(solid_vec, geom.solids[i], list_size + i);
	}
	ListVector::SetListSize(*list_vec, list_size + geom.solids.size());
}

void WriteGeometryVertices(Vector *list_vec, const CompactedGeometry &geom, size_t row) {
	if (geom.vertices.empty()) {
		FlatVector::SetNull(*list_vec, row, true);
		return;
	}
	auto list_size = ListVector::GetListSize(*list_vec);
	FlatVector::GetData<list_entry_t>(*list_vec)[row] = list_entry_t(list_size, geom.vertices.size());
	ListVector::Reserve(*list_vec, list_size + geom.vertices.size());
	auto &struct_vec = ListVector::GetEntry(*list_vec); // STRUCT<x,y,z DOUBLE>
	auto &children = StructVector::GetEntries(struct_vec);
	auto x_data = FlatVector::GetData<double>(*children[0]);
	auto y_data = FlatVector::GetData<double>(*children[1]);
	auto z_data = FlatVector::GetData<double>(*children[2]);
	for (size_t i = 0; i < geom.vertices.size(); i++) {
		x_data[list_size + i] = geom.vertices[i][0];
		y_data[list_size + i] = geom.vertices[i][1];
		z_data[list_size + i] = geom.vertices[i][2];
	}
	ListVector::SetListSize(*list_vec, list_size + geom.vertices.size());
}
```

**Two things to verify against DuckDB's real API before trusting this compiles**,
flagged honestly rather than glossed over: (1) whether `StructVector::GetEntries`
returns entries usable this way when the struct is itself the child of a `ListVector`
(nested struct-inside-list access) — the existing `WriteGeometryProperties` uses
`StructVector::GetEntries` on a **top-level** struct column, not one nested inside a
list; check DuckDB's header (`duckdb/common/types/vector.hpp`, already referenced in
this plan's earlier codex-review research for `duckdb-3d`) for whether
`ListVector::GetEntry` on a `LIST<STRUCT<...>>` column returns a `Vector&` you can then
pass straight into `StructVector::GetEntries`, or whether an extra step is needed. (2)
Whether `ListVector::Reserve`/`GetEntry` calls at each nesting level need to happen in a
specific order relative to sibling reserves (the existing `AppendIntListList` reserves
the grandchild ONCE up front across all entries, rather than per-entry inside the loop —
consider whether `AppendFace`/`AppendShell`/`AppendSolid` above should do the same
up-front batch reserve for efficiency, once correctness is established; not required for
correctness, only performance, so treat as a refactor-while-green candidate, not a
blocker to passing tests first).

- [ ] **Step 4: Commit**

```bash
git add src/include/cityjson/vector_writer.hpp src/cityjson/vector_writer.cpp
git commit -m "feat(arrow-geom): WriteGeometryArrowNative/WriteGeometryVertices"
```

---

### Task 6: Wire into `city_object_utils.cpp` and `scan_function.cpp`

**Files:**
- Modify: `src/include/cityjson/city_object_utils.hpp`, `src/cityjson/city_object_utils.cpp`
- Modify: `src/cityjson/scan_function.cpp`

**Interfaces:**
- Consumes: `ArrowNativeEncoder::Encode` (Task 4), `WriteGeometryArrowNative`/`WriteGeometryVertices` (Task 5).
- Produces: end-to-end `read_cityjson(..., geometry_encoding := 'arrow-native')` writes real, non-empty data.

- [ ] **Step 1: Write the failing end-to-end test**

```sql
# name: test/sql/arrow_native_geometry_roundtrip.test
# description: arrow-native geometry writer produces correct, non-empty data
# group: [cityjson]

require cityjson

statement ok
CREATE TABLE t AS SELECT * FROM read_cityjson('test/fixtures/PLACEHOLDER_SOLID_FIXTURE.city.json', geometry_encoding := 'arrow-native');

query I
SELECT len(geometry_vertices_lod2_2) > 0 FROM t WHERE geometry_lod2_2 IS NOT NULL LIMIT 1;
----
true

# Cross-check: the same object read with the default (wkb) encoding has the
# same face count (via the existing ST_3DNumFaces-equivalent WKB path, or a
# simpler proxy — pick whichever this repo already has a working accessor
# for) as len(geometry_lod2_2[1][1]) under arrow-native (solid -> shell 1 ->
# face list), confirming the compaction produced the right topology, not
# just non-empty data.
```

(Fixture path and the exact cross-check query need confirming against this repo's real
`test/fixtures/` contents and whatever WKB-side face-count accessor already exists in its
SQL test suite — grep an existing `.test` file exercising `ST_3DNumFaces`-equivalent or
similar for the real function name.)

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — `WriteCityObjectRow` doesn't know how to handle
`ColumnType::GeometryArrowNative`/`GeometryVerticesArrowNative` yet (falls through to the
`default:` throw in `WriteToVector`, or — since geometry columns are handled by
`WriteCityObjectRow`'s own `if (col.kind == ...)` chain, not `WriteToVector` — simply
produces null/wrong columns silently; confirm which failure mode you actually see).

- [ ] **Step 3: Add `CityObjectUtils::GetGeometryArrowNative`**

In `city_object_utils.hpp`/`.cpp`, mirroring `GetGeometryWKB` (confirmed signature at
`city_object_utils.hpp:101-103`):

```cpp
static CompactedGeometry GetGeometryArrowNative(const Geometry &geometry,
                                                const std::vector<std::array<double, 3>> &vertices,
                                                const std::optional<Transform> &transform);
```

```cpp
CompactedGeometry CityObjectUtils::GetGeometryArrowNative(const Geometry &geometry,
                                                          const std::vector<std::array<double, 3>> &vertices,
                                                          const std::optional<Transform> &transform) {
	return ArrowNativeEncoder::Encode(geometry, vertices, transform);
}
```

- [ ] **Step 4: Add the `WriteCityObjectRow` branches**

In `src/cityjson/scan_function.cpp` (confirmed structure at lines 1-90), add alongside
the existing `if (col.kind == ColumnType::GeometryWKB) { ... continue; }` block:

```cpp
if (col.kind == ColumnType::GeometryArrowNative) {
	std::optional<Geometry> geom = bind_data.target_lod.has_value()
	                                   ? target_geom
	                                   : city_obj.GetGeometryAtLOD(ParseLODFromGeometryColumn(col.name));
	if (geom.has_value() && vertex_pool != nullptr) {
		auto compacted =
		    CityObjectUtils::GetGeometryArrowNative(geom.value(), *vertex_pool, bind_data.metadata.transform);
		WriteGeometryArrowNative(wrappers[col_idx].AsListMut(), compacted, output_row);
	} else {
		wrappers[col_idx].SetNull(output_row);
	}
	continue;
}

if (col.kind == ColumnType::GeometryVerticesArrowNative) {
	// Same geometry lookup as GeometryArrowNative above — recompute rather than
	// cache across columns for now (this pairs a geometry_vertices_* column
	// with its geometry_* sibling; a later optimisation could compute
	// CompactedGeometry once and write both columns from it, avoiding running
	// ArrowNativeEncoder::Encode twice per row — flagged here as a real,
	// deliberate inefficiency to fix once correctness is established, not a
	// silent oversight).
	std::optional<Geometry> geom = bind_data.target_lod.has_value()
	                                   ? target_geom
	                                   : city_obj.GetGeometryAtLOD(ParseLODFromGeometryColumn(col.name));
	if (geom.has_value() && vertex_pool != nullptr) {
		auto compacted =
		    CityObjectUtils::GetGeometryArrowNative(geom.value(), *vertex_pool, bind_data.metadata.transform);
		WriteGeometryVertices(wrappers[col_idx].AsListMut(), compacted, output_row);
	} else {
		wrappers[col_idx].SetNull(output_row);
	}
	continue;
}
```

Also update `CreateVectors` (`vector_writer.cpp`, confirmed structure at lines 39-60) to
map `ColumnType::GeometryArrowNative`/`GeometryVerticesArrowNative` to `VectorType::List`
in its `if (col.kind == ColumnType::VarcharArray) { ... } else if (...)` chain.

- [ ] **Step 5: Run test to verify it passes**

Expected: PASS. If the double-encode inefficiency noted in Step 4 turns out to matter for
correctness (e.g. if `ArrowNativeEncoder::Encode` isn't perfectly deterministic — it
should be, given no randomness, but confirm) rather than just performance, fix it now
instead of deferring — recompute-per-column is only acceptable if genuinely
side-effect-free and deterministic.

- [ ] **Step 6: Run the FULL existing SQL test suite**

Run this repo's real test command (`make test` or whatever `justfile`/CI actually runs —
confirm rather than guess). Expected: PASS — confirms zero regression to the default WKB
path.

- [ ] **Step 7: Commit**

```bash
git add src/include/cityjson/city_object_utils.hpp src/cityjson/city_object_utils.cpp src/cityjson/scan_function.cpp src/cityjson/vector_writer.cpp test/
git commit -m "feat(scan): wire arrow-native geometry writer into WriteCityObjectRow"
```

---

### Task 7: Encoding-aware `geo` emission

**Files:**
- Modify: `src/cityjson/geoparquet_table_function.cpp`

**Interfaces:**
- Consumes: `GeometryEncoding` (Task 2).
- Produces: `GeoParquetTypeName`-equivalent logic (confirmed at lines ~21-35 of this file, per the design doc's round-2 research) returns GeoParquet-illegal for any column not using WKB, regardless of CM type.

- [ ] **Step 1: Write the failing test**

```sql
statement ok
CREATE TABLE t AS SELECT * FROM read_cityjson('test/fixtures/PLACEHOLDER.city.json', geometry_encoding := 'arrow-native');

query I
SELECT cityjson_geoparquet_geo(...) -- adjust to this repo's real function/table-function call convention for the geo metadata, confirmed to exist as `cityjson_geoparquet_geo` per this plan's earlier research (design doc citation: geoparquet_table_function.cpp)
```

Confirm the real function name/signature by reading `geoparquet_table_function.cpp` in
full (this plan's research only confirmed `GeoParquetTypeName`'s body, not the
table-function wrapper's exact call convention) before writing this test for real.

- [ ] **Step 2: Run test to verify it fails**

Expected: FAIL — an arrow-native `MultiSurface` column is currently declared GeoParquet-legal (design doc round-2 finding, confirmed by reading `GeoParquetTypeName`'s current unconditional `if (cj_type == "MultiSurface" || cj_type == "CompositeSurface") return "MultiPolygon Z";`).

- [ ] **Step 3: Make it encoding-aware**

In `geoparquet_table_function.cpp`'s `GeoParquetTypeName` (confirmed body, lines ~21-35), thread the column's `GeometryEncoding` in and short-circuit:

```cpp
std::string GeoParquetTypeName(const std::string &cj_type, GeometryEncoding encoding) {
	if (encoding != GeometryEncoding::Wkb) {
		return ""; // Arrow-native columns are never GeoParquet-legal, regardless of CM type.
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
	return "";
}
```

Update every call site to pass the column's actual encoding (grep `GeoParquetTypeName(`
— trace back to wherever the bind data's `geometry_encoding` is in scope).

- [ ] **Step 4: Run test to verify it passes**

Run the test, expect PASS. Run the full existing test suite for regression.

- [ ] **Step 5: Commit**

```bash
git add src/cityjson/geoparquet_table_function.cpp test/
git commit -m "fix(geo): geo metadata omits arrow-native-encoded columns regardless of CM type"
```

---

### Task 8: `COPY ... TO (FORMAT PARQUET)` deep-nesting spike (do this EARLY in practice, kept last here only for narrative order)

**Files:** none created — this is a verification task, per the design doc's explicit
call-out: *"verify DuckDB core's `COPY ... TO (FORMAT PARQUET)` round-trips a hand-built
nested `Vector`... with field names and nesting intact. If it renames/collapses/mangles
anything at that depth, the write path needs to change before further investment."*

**This should actually run as early as possible** — ideally right after Task 1, before
investing in Tasks 4-7's encoder/writer work, since a negative result here changes the
whole approach. It's placed last in this document's task numbering only because it
depends on Task 6's writer existing to produce a REAL nested column to test against
end-to-end (a synthetic hand-built `Vector` could be spiked even earlier, standalone, if
the team wants the earliest possible go/no-go — consider doing exactly that as a Task 0
before committing to Tasks 2-7's full implementation, if schedule allows).

- [ ] **Step 1: Round-trip a real arrow-native table through Parquet**

```sql
COPY (SELECT * FROM read_cityjson('test/fixtures/PLACEHOLDER.city.json', geometry_encoding := 'arrow-native')) TO '/tmp/arrow_native_test.parquet' (FORMAT PARQUET);

CREATE TABLE reloaded AS SELECT * FROM '/tmp/arrow_native_test.parquet';

query I
SELECT typeof(geometry_lod2_2) FROM reloaded LIMIT 0;
----
INTEGER[][][][][]
```

- [ ] **Step 2: Verify field names survive, not just types**

`INTEGER[][][][][]`'s `typeof()` output won't reveal whether the intermediate `LIST`
levels kept sensible child field names (`"item"` at each level, matching Arrow/Parquet
convention `cityparquet-rs` also uses) versus DuckDB's Parquet writer renaming/collapsing
them. Use `DESCRIBE`/`PRAGMA`-level introspection or `parquet_schema('/tmp/arrow_native_test.parquet')`
to inspect the actual physical Parquet schema field names at each nesting level, and
compare against what `cityparquet-rs`'s `arrow_native_geometry_data_type()` produces (ask
whoever's running the `cityparquet-rs` plan for a sample file, or generate one yourself
from that plan's Task 1 test fixtures once merged) — this is the actual cross-repo
compatibility check, not just "does DuckDB not crash."

- [ ] **Step 3: Record the result**

If this passes cleanly: no further action, the whole approach is validated at the
Parquet-writer level. If it doesn't: **stop and escalate** — this is exactly the
go/no-go the design doc flagged as a real risk, not a formality; a mismatch here means
either DuckDB's writer needs different handling (e.g. explicit field renaming after
`COPY`) or the whole "share one physical Parquet schema across two independently-branched
C++/Rust producers" premise needs rethinking.

---

## Self-Review

**Spec coverage** (against the design doc + `cityparquet-rs`'s plan, which this plan must match):
- Unified physical shape, exact field nesting order: Task 1, Task 3. ✓ — matches `arrow_native_geometry_data_type()`/`arrow_native_vertices_data_type()` field-for-field.
- Padding dimensions for surface types: Task 4. ✓
- Distinct-source-index compaction (not coordinate dedup): Task 4, tested explicitly. ✓
- Dispatch-by-`geometry_properties.type` invariant: inherited for free — `geometry_properties_lod*` is unchanged, already `GeometryPropertiesStruct`, and this plan never branches on the new columns' physical shape to decide CM type anywhere. ✓
- Ring-closure convention: CityJSON's own boundaries already have no repeated closing index (unlike WKB, which this encoder never touches) — no conversion needed, confirmed by `ArrowNativeEncoder` never adding one. ✓
- `geo` omission: Task 7. ✓
- `geometry_properties` STRUCT: **already done**, discovered mid-plan (commit `d334b26`) — no task needed, corrected the plan's premise instead of adding dead work.
- `duckdb-3d` breakage from the STRUCT change: **out of scope for this plan, in scope for `duckdb-3d`'s own plan** — flagged prominently at the top of this document so whoever reads this plan doesn't miss it, but not duplicated as a task here since it's not this repo's code to fix.
- Deep-nesting `COPY TO PARQUET` spike: Task 8, with an explicit note that it should really run earlier than its position in this document.

**Placeholder scan:** every step has real code; three explicit "confirm against real
source" flags remain (test placement convention in Task 1, `Geometry::boundaries`'s exact
struct shape in Task 4, `GeoParquetTypeName`'s call-site plumbing in Task 7, fixture paths
throughout) — each names exactly what to verify and why, not a disguised "figure it out
later."

**Type consistency:** `CompactedGeometry`/`CompactedSolid`/`CompactedShell`/`CompactedFace`
used identically from Task 4 through Task 6. `GeometryEncoding::{Wkb, ArrowNative}` used
consistently from Task 2 through Task 8. Column names (`geometry_vertices_lod<M>_<m>`)
match `cityparquet-rs`'s plan's naming exactly, per this plan's stated hard constraint.

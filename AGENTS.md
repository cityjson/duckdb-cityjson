# Agent Guide

Workflows and working agreements for the DuckDB CityJSON extension. **What the code
does lives in `docs/`** — see the map below. Keep it that way: describe behaviour
there, describe how we work here.

A **C++20 DuckDB extension** (not Rust). C++20 is pinned via `target_compile_features`;
`std::span` requires it.

## Where things live

| Path | Contents |
| ---- | -------- |
| `src/cityjson_extension.cpp` | Extension loader (`LoadInternal`), registers every function |
| `src/cityjson/` | Implementation |
| `src/include/cityjson/` | Headers — the authority on exact types and signatures |
| `src/external/` | Vendored third-party source. Never formatted, never edited here |
| `test/sql/` | sqllogictest suites — how behaviour is pinned |
| `test/data/` | Fixtures |
| `test/cpp/`, `test/wasm/` | Opt-in harnesses, outside `make test` |
| `.githooks/` | `pre-commit` format + tidy gates (`just hooks` to install) |
| `vcpkg.json` | Dependencies, incl. the `flatcitybuf` git registry |

| Document | Answers |
| -------- | ------- |
| [docs/FUNCTIONS.md](docs/FUNCTIONS.md) | Every SQL function, with worked examples and the output schema |
| [docs/DESIGN_DOC.md](docs/DESIGN_DOC.md) | Architecture: layers, data model, readers, scan path, geometry encodings, appearance, package layer, invariants |
| [docs/TRAPS.md](docs/TRAPS.md) | Implementation traps, per layer. Read before touching a layer |
| [docs/UPDATING.md](docs/UPDATING.md) | Bumping the DuckDB target version |
| `docs/index.html` | Docsify site. GitHub Pages serves `main:/docs`, so a docs fix only goes live once merged to `main` |
| `docs/superpowers/plans/` | Implementation plans |

The parent workspace's `documents/` holds the normative CityParquet specification. It
is the authority on the encoding; this repo implements it.

## How we work

- **Fable advises, Sonnet or Opus executes.** Use Fable as the reviewer or advisor —
  design critique, review, adversarial checks — and an execution-tier model to write
  the code. Bring Fable in before committing to an approach and before declaring done.
- **TDD: red, green, refactor.** Write the failing test, run it and watch it fail for
  the reason you expect, write the minimum to pass, then refactor. A test that has
  never failed has not been shown to test anything.
- **The pre-commit hook runs both CI code-quality gates.** `just hooks` once per
  clone (`core.hooksPath` is local config and is not inherited). It formats and
  re-stages, then runs clang-tidy over the staged `.cpp` and **blocks the commit on
  any finding** — CI's `Tidy Check` takes ~37 minutes, so finding out there is late.
  It needs `build/tidy/compile_commands.json`, which the cmake configure inside
  `make tidy-check` writes, and costs ~10s per staged file. `SKIP_TIDY=1` (or
  `SKIP_FORMAT=1`) bypasses one phase, `--no-verify` both.
- **Version pinning differs between the two phases, deliberately.** Formatting is
  skipped outright unless clang-format is exactly 11.0.1 — another version reformats
  conforming code its own way and churns the diff forever. Tidy *prefers* CI's 18.x
  but accepts newer and says so, because clang-tidy 18 cannot parse a current macOS
  SDK's libc++ at all (it needs clang 19+ builtins) and would be unusable here. CI
  stays the authority; `bugprone-unchecked-optional-access` in particular models
  dataflow differently across releases, so a newer local pass is not a CI pass.
  On macOS the hook adds `-isysroot`: the compile database records AppleClang's
  command line, and a Homebrew clang-tidy cannot find `<string>` without it.
- **Breaking changes are welcome.** Prefer the correct interface over the compatible
  one. Say so in the commit message with a `!` and describe the migration.
- **Document the present, not the past.** No "fixed", "previously broken", "used to
  be". Nobody needs the old behaviour; they need today's. Write history only when
  explicitly asked — a commit message or release note is where it belongs.

## Build and test

```sh
GEN=ninja make                 # first configure
just rebuild                   # incremental: extension + duckdb CLI + unittest
just build                     # full release build
make test                      # the sqllogictest suite
```

**`just rebuild` (or the `unittest` target) is not optional.** `make test` runs
`build/release/test/unittest` but does **not** rebuild it, so omitting the target
tests a stale binary and reports green on code that never ran.

Interactive check:

```sh
./build/release/duckdb -c "SELECT * FROM read_cityjson('test/data/minimal.city.json');"
```

### Opt-in harnesses

None of these run under `make test`.

```sh
FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_encoder_tests.sh
FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_fcb_selective_tests.sh
just test-remote               # HTTP reads + cross-format parity (~25 MB)
just test-fcb-remote           # FCB HTTP range reads
just test-wasm                 # needs `just wasm` first
```

`FCB_PREFIX` points at a prefix holding flatcitybuf + flatbuffers — `just vendor-fcb`
builds one at `.vendor/prefix`, and a configured tree already has one under
`build/release/vcpkg_installed/<triplet>`. The C++ harnesses link
`build/release/src/libduckdb.{so,dylib}`, which the usual targets do not rebuild;
undefined `fcb::` or `nlohmann` symbols mean it is stale — `ninja -C build/release
src/libduckdb.so`.

### Formatting

```sh
pip install 'clang_format==11.0.1' 'black==24.*' cmake-format
make format-check              # what CI runs
make format-fix
```

**The versions are exact.** CI installs `clang_format==11.0.1` and `black==24.*`. A
different clang-format reformats already-conforming code its own way, so it fights CI
and churns the diff. The hook detects a mismatch and skips with a warning rather than
formatting.

The formatter walks `src` and `test` wholesale and cannot be told to skip a directory,
so it will reformat `src/external/`. Move it aside for a manual `make format-fix`; the
hook already excludes it.

## Writing tests

- **A sqllogictest header is exactly three lines**: `# name:`, `# description:`,
  `# group:`, with a **single-line** description. A multi-line description makes the
  formatter hoist `# group:` up and orphan the rest. Long explanations go in a comment
  block after the header.
- **Network tests are `require-env` gated** and must run `set ignore_error_messages`.
  The runner's default skip-list is `{"HTTP", "Unable to connect"}`, so without it a
  transport failure reports as a *skip* and the test passes while testing nothing.
  They also need explicit `INSTALL httpfs; LOAD httpfs` — the runner disables
  autoloading, and `require httpfs` would skip the whole file.
- **Prefer fixtures this extension did not produce.** A test that writes its source
  with the writer under test and reads it back with the matching reader is a circular
  oracle: a reader and writer agreeing on a wrong encoding pass every assertion.
  `test/sql/cityjson_corpus_parity.test` uses upstream-produced fixtures for this
  reason.
- **File-level assertions where row-level ones cannot see.** A value can re-parse
  identically while the file on disk has degraded, so pair `EXCEPT` checks with
  `read_text` ones.
- Use `EXCEPT ALL`, not `EXCEPT`, for row parity — plain `EXCEPT` deduplicates and
  hides multiplicity drift.

## Adding to the code

- **A new named parameter**: register it in `table_function_registration.cpp`, parse it
  in the bind (`bind_function.cpp`), store it on `CityJSONBindData`, use it in
  `scan_function.cpp`.
- **A new predefined column**: add to `GetDefinedColumns()` (`column_types.cpp`),
  handle it in `CityObjectUtils::GetAttributeValue()`, and check whether
  `IsPredefinedColumn()` needs it.
- Read [docs/TRAPS.md](docs/TRAPS.md) for the layer first.

## References

- CityJSON specification: <https://www.cityjson.org/specs/2.0.1/>
- CityJSONSeq: <https://www.cityjson.org/cityjsonseq/>
- DuckDB extension development: <https://duckdb.org/docs/stable/dev/extensions>

# justfile — shortcuts for building, testing, and running CI checks locally.
# Run `just` with no arguments to list all recipes. Requires: just, cmake, ninja,
# python3, and clang-format/clang-tidy 11.x for the format/tidy recipes.
#
# The CI pipeline (.github/workflows/MainDistributionPipeline.yml) runs two things
# via duckdb/extension-ci-tools: (1) build + test, (2) format-check + tidy-check.
# The recipes below mirror those so you can reproduce CI before pushing.

# Build a fast release binary. Optional: pass CORE_EXTENSIONS="httpfs" etc. via env.
build_targets := "cityjson_extension cityjson_loadable_extension duckdb unittest"

# Wasm toolchain pins, mirroring the CI distribution pipeline.
# emsdk: extension-ci-tools v1.5.4 `_extension_distribution.yml` pins
# emscripten-core/setup-emsdk@v13 with version 3.1.71.
# vcpkg baseline: the `builtin-baseline` commit in vcpkg.json — a plain shallow clone
# does NOT contain it, so `wasm-setup` fetches it explicitly (manifest resolution fails
# otherwise).
emsdk_version := "3.1.71"
vcpkg_baseline := "84bab45d415d22042bd0b9081aea57f362da3f35"

# Install the repo's git hooks (pre-commit auto-formats staged files with the
# CI-pinned tools). One command per clone; core.hooksPath is local config, so it
# is not inherited automatically. Needs the formatters CI pins:
#   pip install 'clang_format==11.0.1' 'black==24.*' cmake-format
# The hook skips itself with a warning if they are missing or the wrong version,
# rather than blocking your commit or churning the diff.
hooks:
    git config core.hooksPath .githooks
    @echo "core.hooksPath -> .githooks"

# The vcpkg toolchain file, which every native configure needs.
#
# `find_package(nlohmann_json REQUIRED)` and `find_package(flatcitybuf CONFIG REQUIRED)`
# in CMakeLists.txt are both config-only, and flatcitybuf resolves solely from the
# HideBa/vcpkg git registry declared in vcpkg.json. Neither is reachable unless CMake is
# configured with vcpkg's toolchain file, which is what puts VCPKG_MANIFEST_DIR on the
# command line and makes the manifest install run at all. CI gets this from the
# VCPKG_TOOLCHAIN_PATH it sets on every job; the recipes below derive the same path from
# VCPKG_ROOT so a local `just build` mirrors CI without a per-shell export.
#
# An explicit VCPKG_TOOLCHAIN_PATH in the environment wins. With neither variable set
# this is the empty string, which extension-ci-tools' Makefile treats exactly as unset.
vcpkg_root := env_var_or_default("VCPKG_ROOT", "")
vcpkg_toolchain := env_var_or_default("VCPKG_TOOLCHAIN_PATH", if vcpkg_root == "" { "" } else { vcpkg_root / "scripts/buildsystems/vcpkg.cmake" })

# List available recipes (default).
default:
    @just --list

# Full release build (configures the project the first time; slow on a clean tree).
build:
    VCPKG_TOOLCHAIN_PATH="{{vcpkg_toolchain}}" GEN=ninja make release

# Full debug build.
debug:
    VCPKG_TOOLCHAIN_PATH="{{vcpkg_toolchain}}" GEN=ninja make debug

# Fast incremental rebuild of the extension, duckdb CLI, and test binary.
# Use this in the edit-build-test loop after `just build` has configured the tree.
rebuild:
    cmake --build build/release --target {{build_targets}}

# Build with FlatCityBuf (.fcb) support enabled.
build-fcb:
    VCPKG_TOOLCHAIN_PATH="{{vcpkg_toolchain}}" EXT_FLAGS="-DCITYJSON_ENABLE_FCB=ON" GEN=ninja make release

# Build flatbuffers + flatcitybuf (tag cpp-v0.9.0) into .vendor/prefix.
# Re-run after a tag bump: the recipe re-checks out the pinned tag and drops the
# stale build tree, so an existing .vendor/src clone upgrades instead of lingering.
# The loadable extension is a shared object, so both static libs need PIC.
# Point CMake at the result with -Dflatcitybuf_DIR / -Dflatbuffers_DIR (or export
# CMAKE_PREFIX_PATH="$(pwd)/.vendor/prefix"); test/cpp/run_fcb_selective_tests.sh
# wants FCB_PREFIX="$(pwd)/.vendor/prefix".
vendor-fcb:
    #!/usr/bin/env bash
    set -euo pipefail
    PREFIX="$(pwd)/.vendor/prefix"
    SRC="$(pwd)/.vendor/src"
    mkdir -p "$SRC"
    if [ ! -d "$SRC/flatbuffers" ]; then
        git clone --depth 1 --branch v25.9.23 https://github.com/google/flatbuffers "$SRC/flatbuffers"
    fi
    cmake -S "$SRC/flatbuffers" -B "$SRC/flatbuffers/build" -DCMAKE_BUILD_TYPE=Release \
        -DFLATBUFFERS_BUILD_TESTS=OFF -DFLATBUFFERS_BUILD_FLATC=OFF -DFLATBUFFERS_BUILD_FLATHASH=OFF \
        -DFLATBUFFERS_BUILD_FLATLIB=ON -DFLATBUFFERS_BUILD_SHAREDLIB=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_PREFIX="$PREFIX"
    cmake --build "$SRC/flatbuffers/build" -j && cmake --install "$SRC/flatbuffers/build"
    if [ ! -d "$SRC/flatcitybuf" ]; then
        git clone --depth 1 --branch cpp-v0.9.0 https://github.com/cityjson/flatcitybuf "$SRC/flatcitybuf"
    else
        git -C "$SRC/flatcitybuf" fetch --depth 1 origin tag cpp-v0.9.0
        git -C "$SRC/flatcitybuf" checkout cpp-v0.9.0
    fi
    rm -rf "$SRC/flatcitybuf/build"
    cmake -S "$SRC/flatcitybuf/src/cpp" -B "$SRC/flatcitybuf/build" -DCMAKE_BUILD_TYPE=Release \
        -DFCB_WITH_JSON=ON -DFCB_WITH_CURL=OFF -DFCB_BUILD_TESTS=OFF -DFCB_BUILD_EXAMPLES=OFF \
        -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_PREFIX_PATH="$PREFIX" -DCMAKE_INSTALL_PREFIX="$PREFIX"
    cmake --build "$SRC/flatcitybuf/build" -j && cmake --install "$SRC/flatcitybuf/build"

# Installs the pinned emsdk and a durable vcpkg checkout under the gitignored
# .vendor/. Idempotent — safe to re-run. ~10 min and ~2 GB the first time (emsdk
# downloads ~340 MB; the vcpkg clone is shallow, but vcpkg then fetches the
# flatcitybuf git registry in full).

# One-time toolchain bootstrap for `just wasm` (emsdk + vcpkg into .vendor/).
wasm-setup:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p .vendor
    if [ ! -d .vendor/emsdk ]; then
        git clone --depth 200 https://github.com/emscripten-core/emsdk .vendor/emsdk
    fi
    (cd .vendor/emsdk && ./emsdk install {{emsdk_version}} && ./emsdk activate {{emsdk_version}})
    if [ ! -d .vendor/vcpkg ]; then
        git clone --depth 1 https://github.com/microsoft/vcpkg .vendor/vcpkg
        (cd .vendor/vcpkg && ./bootstrap-vcpkg.sh -disableMetrics)
    fi
    # The shallow clone lacks vcpkg.json's builtin-baseline commit; without this the
    # manifest fails to resolve ("could not find baseline").
    git -C .vendor/vcpkg fetch --depth 1 origin {{vcpkg_baseline}}

# The local equivalent of CI's wasm distribution jobs. Run `just wasm-setup` once
# first. The first build is slow (vcpkg compiles flatcitybuf and friends for
# wasm32-emscripten); later builds reuse those binaries. The output lands in
# build/<flavour>/extension/cityjson/cityjson.duckdb_extension.wasm and the native
# build/release tree is untouched.
#
# Which flavour you want depends on who loads the result:
#
#   wasm_mvp   what `just test-wasm` asserts against, and what test/wasm/smoke.mjs
#              offers duckdb-wasm. Cannot be loaded by an `eh` instance.
#   wasm_eh    what a *browser* loads. duckdb-wasm's selectBundle() picks the eh
#              bundle wherever native wasm exceptions are available, which is
#              everywhere that matters, and an eh instance takes only eh
#              extensions. It is also the only flavour that can report errors:
#              the mvp bundle references `_setThrew` without defining it, so the
#              first C++ exception surfaces as a ReferenceError rather than the
#              actual message (see docs/TRAPS.md).
#
# NOTE: no GEN=ninja here, unlike every other build recipe — and CI does not set it
# for wasm either. The wasm targets in extension-ci-tools' duckdb_extension.Makefile
# hardcode `emmake make -j8 -Cbuild/<flavour>`, so a Ninja-generated tree has no
# makefile and the build dies with "No targets specified and no makefile found".
# Recovering also needs `rm -rf build/<flavour>`, since CMake refuses to switch
# generator in an existing cache.

# Build the DuckDB-Wasm extension with the pinned emsdk + .vendor/vcpkg.
wasm flavour="wasm_mvp":
    #!/usr/bin/env bash
    set -euo pipefail
    case "{{flavour}}" in
        wasm_mvp|wasm_eh|wasm_threads) ;;
        *) echo "unknown wasm flavour: {{flavour}} (want wasm_mvp, wasm_eh or wasm_threads)" >&2; exit 2 ;;
    esac
    source .vendor/emsdk/emsdk_env.sh
    VCPKG_TOOLCHAIN_PATH="$(pwd)/.vendor/vcpkg/scripts/buildsystems/vcpkg.cmake" make {{flavour}}
    echo "-> build/{{flavour}}/extension/cityjson/cityjson.duckdb_extension.wasm"

# Run the full SQL test suite (assumes a build exists; run `just rebuild` first).
test:
    ./build/release/test/unittest "test/sql/*"

# Rebuild then run the full suite — the common inner-loop command.
t: rebuild test

# Run a single test file, e.g. `just test-file test/sql/cityjson_delft_e2e.test`.
test-file FILE:
    ./build/release/test/unittest "{{FILE}}"

# Network-gated FlatCityBuf remote-read tests (HTTP range requests). Skipped by the
# plain `just test` run because FCB_REMOTE_TEST_URL is unset there. The default URL is
# a 2.3 GB file: the test only reads its header and a 500 m bbox, so pass a different
# url= only with a bbox that lands inside it (see the test file's header comment).
test-fcb-remote url="https://flatcitybuf.open3d.city/data/3dbag_subset.city.fcb":
    FCB_REMOTE_TEST_URL={{url}} ./build/release/test/unittest "test/sql/cityjson_fcb_remote.test"

# HTTP read tests against the open3d.city datasets. Two files:
#   cityjson_remote.test        transport -- every reader and metadata function
#                               over HTTP, plus a hosted CityParquet package
#                               (~25 MB of downloads, full Delft)
#   cityjson_corpus_parity.test semantics -- the SAME 3 features in all four
#                               formats, produced by UPSTREAM tooling, so a
#                               reader disagreement is evidence about us rather
#                               than a circular oracle (~93 KB)
# Opt-in: `make test` skips both entirely.
test-remote:
    CITYJSON_REMOTE_TEST=1 ./build/release/test/unittest "test/sql/cityjson_remote.test"
    CITYJSON_REMOTE_TEST=1 ./build/release/test/unittest "test/sql/cityjson_corpus_parity.test"

# The docs/TESTING.md notebook walkthrough, end to end: read every format, build a
# CityParquet package, insert a second file into it, write it out and read it back,
# then the FlatCityBuf spatial and attribute filters. Downloads nothing in advance
# -- every fixture is fetched by the query that needs it, which costs around 120 MB
# of transfer for 25 MB of data, because nothing is cached between queries.
# Opt-in; `make test` skips it.
#
# The geoparquet file additionally needs `spatial` (INSTALL spatial), and is the
# one assertion with real teeth: Delft's LoD0 decoded from the Seq must equal
# Delft's LoD0 decoded from the document. It is a separate file because `require
# spatial` skips a whole file rather than one query.
test-notebook:
    CITYJSON_NOTEBOOK_TEST=1 ./build/release/test/unittest "test/sql/cityjson_notebook_e2e.test"
    CITYJSON_NOTEBOOK_TEST=1 ./build/release/test/unittest "test/sql/cityjson_notebook_geoparquet.test"

# Runtime smoke test of the wasm build under Node + @duckdb/duckdb-wasm. Opt-in like
# the other extra harnesses (`make test` never runs it); needs `just wasm` first, and
# network access on the first run to populate test/wasm/node_modules. Set
# FCB_REMOTE_TEST_URL to additionally exercise a bbox-filtered HTTP range read.
test-wasm:
    test/wasm/run_wasm_smoke.sh

# Run the extension's canonical test target the same way CI does (builds if needed).
test-ci:
    make test

# Validate a GLB export of the Delft tile with the Khronos glTF validator (needs node/npm
# and network for the tile). Zero errors is the bar; warnings are printed for reading.
# The npm package "gltf-validator" ships no CLI (no `bin` in its package.json, checked
# against 2.0.0-dev.3.10) -- `npx gltf-validator` always fails with "could not determine
# executable to run", regardless of network. Its documented entry point is the JS
# `validateBytes` API, so that is what this recipe drives.
# Opt-in: not part of `make test` or `just ci`.
test-gltf-validate:
    #!/usr/bin/env bash
    set -euo pipefail
    OUT="build/gltf_validate"
    mkdir -p "$OUT"
    ./build/release/duckdb -c "INSTALL httpfs; LOAD httpfs; \
      COPY (SELECT * FROM read_cityjsonseq('https://cityjson.open3d.city/cityjsonseq/delft.city.jsonl')) \
      TO '$OUT/delft.glb' (FORMAT glb, lod '2.2', attributes true);"
    npm install --no-save --prefix "$OUT" gltf-validator >/dev/null
    NODE_PATH="$OUT/node_modules" node -e '
      const fs = require("fs");
      const { validateBytes } = require("gltf-validator");
      validateBytes(new Uint8Array(fs.readFileSync(process.argv[1])))
        .then((report) => fs.writeFileSync(process.argv[2], JSON.stringify(report)))
        .catch((error) => { console.error(error); process.exit(1); });
    ' "$OUT/delft.glb" "$OUT/report.json"
    python3 - "$OUT/report.json" <<'EOF'
    import json, sys
    r = json.load(open(sys.argv[1]))
    issues = r.get("issues", {})
    print("errors:", issues.get("numErrors"), "warnings:", issues.get("numWarnings"))
    for m in issues.get("messages", [])[:20]:
        print(" ", m.get("severity"), m.get("code"), m.get("pointer"))
    sys.exit(0 if issues.get("numErrors", 1) == 0 else 1)
    EOF

# Compare an OBJ export of the Delft tile with cjio's (pip install cjio). cjio triangulates
# and writes world coordinates; ours is exported with `origin 'none'` so it also writes
# world coordinates, and only the vertex sets are then comparable: both must contain the
# same distinct (x, y, z) triples.
#
# Both sides are pinned to lod 2.2, and both are cleaned to just that lod's vertices:
# cjio's OBJ writer emits the file's *entire* global vertex pool (every lod, orphans
# included) ahead of the per-object faces, so `lod_filter 2.2 vertices_clean` first
# restricts it to what lod 2.2 actually uses; our writer already emits only the
# vertices the requested lod references. Full precision (the `precision` default of
# 17, i.e. no `precision` option here) avoids rounding twice -- Delft's `.3f` cjio
# format and this recipe's `round(…, 3)` follow from the tile's `transform.scale` of
# 0.001; a tile with a coarser or finer scale needs that digit adjusted to match.
# Opt-in: not part of `make test` or `just ci`.
test-obj-cjio:
    #!/usr/bin/env bash
    set -euo pipefail
    OUT="build/obj_cjio"
    mkdir -p "$OUT"
    curl -sSL -o "$OUT/delft.city.json" https://cityjson.open3d.city/cityjson/delft.city.json
    cjio "$OUT/delft.city.json" lod_filter 2.2 vertices_clean export obj "$OUT/cjio.obj"
    ./build/release/duckdb -c "COPY (SELECT * FROM read_cityjson('$OUT/delft.city.json', lod := '2.2')) TO '$OUT/ours.obj' (FORMAT obj, origin 'none', lod '2.2');"
    python3 - "$OUT/cjio.obj" "$OUT/ours.obj" <<'EOF'
    import sys
    def verts(p):
        s = set()
        for line in open(p):
            if line.startswith("v "):
                x, y, z = (round(float(t), 3) for t in line.split()[1:4])
                s.add((x, y, z))
        return s
    a, b = verts(sys.argv[1]), verts(sys.argv[2])
    print("cjio", len(a), "ours", len(b), "only-cjio", len(a - b), "only-ours", len(b - a))
    sys.exit(0 if a == b else 1)
    EOF

# clang-format check — matches the CI "Format Check" job (scans src and test).
format-check:
    make format-check

# Apply clang-format fixes in place (src + test).
format-fix:
    make format-fix

# clang-tidy check — matches the CI "Tidy Check" job (needs a vcpkg toolchain to configure).
tidy:
    VCPKG_TOOLCHAIN_PATH="{{vcpkg_toolchain}}" make tidy-check

# Reproduce the CI gates locally: format check, then build, then test.
ci: format-check build test

# Remove all build artifacts.
clean:
    make clean

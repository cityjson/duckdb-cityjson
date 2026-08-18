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

# List available recipes (default).
default:
    @just --list

# Full release build (configures the project the first time; slow on a clean tree).
build:
    GEN=ninja make release

# Full debug build.
debug:
    GEN=ninja make debug

# Fast incremental rebuild of the extension, duckdb CLI, and test binary.
# Use this in the edit-build-test loop after `just build` has configured the tree.
rebuild:
    cmake --build build/release --target {{build_targets}}

# Build with FlatCityBuf (.fcb) support enabled.
build-fcb:
    EXT_FLAGS="-DCITYJSON_ENABLE_FCB=ON" GEN=ninja make release

# Build flatbuffers + flatcitybuf (tag cpp-v0.9.0) into .vendor/prefix.
# Re-run after a tag bump: the recipe re-checks out the pinned tag and drops the
# stale build tree, so an existing .vendor/src clone upgrades instead of lingering.
# The loadable extension is a shared object, so both static libs need PIC.
# Point CMake at the result with -Dflatcitybuf_DIR / -Dflatbuffers_DIR (or export
# CMAKE_PREFIX_PATH="$(pwd)/.vendor/prefix"); test/cpp/run_encoder_tests.sh wants
# FCB_PREFIX="$(pwd)/.vendor/prefix".
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

# The local equivalent of CI's wasm_mvp distribution job. Run `just wasm-setup` once
# first. The first build is slow (vcpkg compiles flatcitybuf and friends for
# wasm32-emscripten); later builds reuse those binaries. The output lands in
# build/wasm_mvp/extension/cityjson/cityjson.duckdb_extension.wasm and the native
# build/release tree is untouched.
#
# NOTE: no GEN=ninja here, unlike every other build recipe — and CI does not set it
# for wasm either. The wasm targets in extension-ci-tools' duckdb_extension.Makefile
# hardcode `emmake make -j8 -Cbuild/wasm_mvp`, so a Ninja-generated tree has no
# makefile and the build dies with "No targets specified and no makefile found".

# Build the DuckDB-Wasm extension (wasm_mvp flavour) with the pinned emsdk + .vendor/vcpkg.
wasm:
    #!/usr/bin/env bash
    set -euo pipefail
    source .vendor/emsdk/emsdk_env.sh
    VCPKG_TOOLCHAIN_PATH="$(pwd)/.vendor/vcpkg/scripts/buildsystems/vcpkg.cmake" make wasm_mvp

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

# Runtime smoke test of the wasm build under Node + @duckdb/duckdb-wasm. Opt-in like
# the other extra harnesses (`make test` never runs it); needs `just wasm` first, and
# network access on the first run to populate test/wasm/node_modules. Set
# FCB_REMOTE_TEST_URL to additionally exercise a bbox-filtered HTTP range read.
test-wasm:
    test/wasm/run_wasm_smoke.sh

# Run the extension's canonical test target the same way CI does (builds if needed).
test-ci:
    make test

# clang-format check — matches the CI "Format Check" job (scans src and test).
format-check:
    make format-check

# Apply clang-format fixes in place (src + test).
format-fix:
    make format-fix

# clang-tidy check — matches the CI "Tidy Check" job (needs a vcpkg toolchain to configure).
tidy:
    make tidy-check

# Reproduce the CI gates locally: format check, then build, then test.
ci: format-check build test

# Remove all build artifacts.
clean:
    make clean

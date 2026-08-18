#!/usr/bin/env bash
# Compile and run the FlatCityBuf selective-deserialisation assertions
# (FcbFieldMask / ConvertFeatureLight / DecodeAttributesFiltered).
#
# Like run_encoder_tests.sh next to it, this is NOT wired into `make test`: it
# needs a built build/release (for libduckdb) and the flatcitybuf install prefix
# that build was configured with. Run it directly, from the repo root:
#
#   FCB_PREFIX="$(pwd)/.vendor/prefix" test/cpp/run_fcb_selective_tests.sh
#
# FCB_PREFIX may equally be the vcpkg prefix the build actually used, which is
# where both the flatcitybuf and nlohmann headers live when the dependency is
# resolved through the manifest rather than `just vendor-fcb`:
#
#   FCB_PREFIX="$(pwd)/build/release/vcpkg_installed/<triplet>" test/cpp/run_fcb_selective_tests.sh
#
# STALE-libduckdb TRAP: this links -lduckdb against build/release/src/libduckdb.<so|dylib>,
# which the three usual build targets (cityjson_extension, cityjson_loadable_extension,
# duckdb) do NOT rebuild. If the link fails with undefined fcb::/nlohmann symbols
# (or json_abi_v3_11_3 mangling mismatches), the library predates the current sources:
#
#   ninja -C build/release src/libduckdb.<so|dylib>
#
# The check below warns when that is the likely cause, rather than leaving you to
# read a wall of undefined symbols.
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"

: "${FCB_PREFIX:?set FCB_PREFIX to the flatcitybuf install prefix (e.g. \$(pwd)/.vendor/prefix)}"

# The shared library is .dylib on macOS and .so elsewhere; hardcoding one name
# made this guard silently never fire on the other platform, which is precisely
# when a stale library is hardest to diagnose.
LIBDUCKDB=""
for _cand in "$REPO/build/release/src/libduckdb.dylib" "$REPO/build/release/src/libduckdb.so"; do
  if [ -f "$_cand" ]; then LIBDUCKDB="$_cand"; break; fi
done
EXT_ARCHIVE="$REPO/build/release/extension/cityjson/libcityjson_extension.a"
if [ -n "$LIBDUCKDB" ] && [ -f "$EXT_ARCHIVE" ] && [ "$EXT_ARCHIVE" -nt "$LIBDUCKDB" ]; then
  echo "WARNING: $LIBDUCKDB is older than $EXT_ARCHIVE."
  echo "         If the link below fails on undefined fcb::/nlohmann symbols, run:"
  echo "           ninja -C $REPO/build/release $(basename "$(dirname "$LIBDUCKDB")")/$(basename "$LIBDUCKDB")"
fi

rm -f "$HERE/test_fcb_selective"

g++ -std=c++20 -g \
  -DCITYJSON_HAS_FCB -DFCB_WITH_JSON=1 \
  -I"$REPO/src/include" \
  -I"$REPO/duckdb/src/include" \
  -I"$FCB_PREFIX/include" \
  "$HERE/test_fcb_selective.cpp" \
  "$REPO/src/cityjson/fcb_selective_convert.cpp" \
  "$REPO/src/cityjson/cityjson_types.cpp" \
  "$REPO/src/cityjson/json_utils.cpp" \
  "$REPO/src/cityjson/column_types.cpp" \
  "$REPO/src/cityjson/lod_table.cpp" \
  "$REPO/src/cityjson/types.cpp" \
  "$REPO/src/cityjson/error.cpp" \
  -L"$REPO/build/release/src" -lduckdb -Wl,-rpath,"$REPO/build/release/src" \
  "$FCB_PREFIX/lib/libfcb_core_cpp.a" "$FCB_PREFIX/lib/libflatbuffers.a" \
  -o "$HERE/test_fcb_selective" 2>&1 | head -60

# pipefail makes ${PIPESTATUS[0]} redundant, but be explicit: without this a failed
# compile would leave the previous binary in place and the run below would report
# success for code that no longer builds.
if [ ! -x "$HERE/test_fcb_selective" ]; then
  echo "COMPILE FAILED"
  exit 1
fi

# The fixtures are addressed relative to the repo root.
CITYJSON_TEST_DATA="$REPO/test/data" "$HERE/test_fcb_selective"

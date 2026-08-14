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
# STALE-libduckdb TRAP: this links -lduckdb against build/release/src/libduckdb.so,
# which the three usual build targets (cityjson_extension, cityjson_loadable_extension,
# duckdb) do NOT rebuild. If the link fails with undefined fcb::/nlohmann symbols
# (or json_abi_v3_11_3 mangling mismatches), the .so predates the current sources:
#
#   ninja -C build/release src/libduckdb.so
#
# The check below warns when that is the likely cause, rather than leaving you to
# read a wall of undefined symbols.
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"

: "${FCB_PREFIX:?set FCB_PREFIX to the flatcitybuf install prefix (e.g. \$(pwd)/.vendor/prefix)}"

LIBDUCKDB="$REPO/build/release/src/libduckdb.so"
EXT_ARCHIVE="$REPO/build/release/extension/cityjson/libcityjson_extension.a"
if [ -f "$LIBDUCKDB" ] && [ -f "$EXT_ARCHIVE" ] && [ "$EXT_ARCHIVE" -nt "$LIBDUCKDB" ]; then
  echo "WARNING: $LIBDUCKDB is older than $EXT_ARCHIVE."
  echo "         If the link below fails on undefined fcb::/nlohmann symbols, run:"
  echo "           ninja -C $REPO/build/release src/libduckdb.so"
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

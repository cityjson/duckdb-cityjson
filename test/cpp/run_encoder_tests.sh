#!/usr/bin/env bash
# Compile and run the ArrowNativeEncoder assertions.
#
# This repo's test convention is SQL-level tests in test/sql/, and the encoder is
# covered there end-to-end. This harness exists alongside that because the
# compaction rules -- padding dimensions, and compaction by distinct source index
# rather than by coordinate value -- are cheaper to pin directly than to infer
# from a round-trip, and a SQL test cannot reach a malformed input the reader
# would reject first.
#
# It is not wired into `make test`: it needs a built build/release (for libduckdb)
# and the flatcitybuf prefix that build was configured with. Run it directly:
#
#   FCB_PREFIX=/path/to/prefix test/cpp/run_encoder_tests.sh
#
set -uo pipefail

REPO="$(cd "$(dirname "$0")/../.." && pwd)"
HERE="$(cd "$(dirname "$0")" && pwd)"

g++ -std=c++20 -g \
  -I"$REPO/src/include" \
  -I"$REPO/duckdb/src/include" \
  "$HERE/test_arrow_native_encoder.cpp" \
  "$REPO/src/cityjson/arrow_native_encoder.cpp" \
  "$REPO/src/cityjson/error.cpp" \
  "$HERE/transform_stub.cpp" \
  -L"$REPO/build/release/src" -lduckdb -Wl,-rpath,"$REPO/build/release/src" \
  "${FCB_PREFIX:?set FCB_PREFIX to the flatcitybuf install prefix}/lib/libfcb_core_cpp.a" "${FCB_PREFIX:?set FCB_PREFIX to the flatcitybuf install prefix}/lib/libflatbuffers.a" \
  -o "$HERE/test_arrow_native_encoder" 2>&1 | head -40

if [ ! -x "$HERE/test_arrow_native_encoder" ]; then
  echo "COMPILE FAILED"
  exit 1
fi

"$HERE/test_arrow_native_encoder"

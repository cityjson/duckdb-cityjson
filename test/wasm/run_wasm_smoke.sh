#!/usr/bin/env bash
# Runtime smoke test of the DuckDB-Wasm extension build under Node.
#
# Opt-in, like test/cpp/run_encoder_tests.sh and the remote FCB test: `make test`
# never runs it. Needs `just wasm` to have produced the artifact, and network access
# the first time (to populate test/wasm/node_modules).
#
#   just test-wasm
#   FCB_REMOTE_TEST_URL=https://flatcitybuf.open3d.city/data/3dbag_subset.city.fcb just test-wasm
#
# Override the artifact under test with CITYJSON_WASM_EXT=/path/to/*.duckdb_extension.wasm.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"

EXT="${CITYJSON_WASM_EXT:-$REPO/build/wasm_mvp/extension/cityjson/cityjson.duckdb_extension.wasm}"
if [ ! -f "$EXT" ]; then
    echo "missing wasm extension: $EXT" >&2
    echo "build it first:  just wasm      (one-time setup: just wasm-setup)" >&2
    exit 2
fi

if ! command -v node >/dev/null 2>&1; then
    echo "node not found on PATH" >&2
    exit 2
fi

# duckdb-wasm's Node path needs a modern V8 (BigInt64Array, WebAssembly dynamic
# linking). 18 is the floor duckdb-wasm itself supports.
node_major="$(node -p 'process.versions.node.split(".")[0]')"
if [ "$node_major" -lt 18 ]; then
    echo "node $(node --version) is too old; need >= 18" >&2
    exit 2
fi

cd "$HERE"
# `npm ci` needs the lockfile; fall back to `npm install` if it is somehow absent.
if [ -f package-lock.json ]; then
    npm ci --no-audit --no-fund
else
    npm install --no-audit --no-fund
fi

exec node smoke.mjs

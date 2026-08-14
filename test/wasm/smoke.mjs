#!/usr/bin/env node
//
// Runtime smoke test for the DuckDB-Wasm build of the cityjson extension.
//
// Loads `build/wasm_mvp/extension/cityjson/cityjson.duckdb_extension.wasm` into a
// real DuckDB-Wasm instance under Node and asserts that the table functions return
// the same answers the native build does. Opt-in, like `test/cpp/*` and the remote
// FCB test -- `make test` never runs it. Drive it with `just test-wasm`.
//
// Oracle values are hardcoded below, each with the native command that produced it.
//
// ---------------------------------------------------------------------------
// Three things about DuckDB-Wasm under Node that are not in its README, all of
// them discovered empirically against @duckdb/duckdb-wasm 1.33.1-dev57.0. Each has
// its own comment at the point of use; summarised here because together they are
// the whole reason this file is longer than "open a db and run a query":
//
//  1. Use the *blocking* Node entrypoint (`@duckdb/duckdb-wasm/blocking`), not the
//     worker-based `AsyncDuckDB`. The shipped `duckdb-node-*.worker.cjs` files are
//     CommonJS, and the `web-worker` shim evaluates a classic worker through
//     `vm.runInThisContext`, where `module` is undefined -- so `new Worker(f)`
//     dies with `ReferenceError: module is not defined`. (`{ type: 'module' }`
//     works around that, but buys nothing here: everything below behaves
//     identically on both paths, and the blocking bindings need no shim at all.)
//
//  2. Extension loading goes through a *cache directory*, never through a
//     registered file buffer. See seedExtensionCache() below.
//
//  3. The `wasm_mvp` bundle cannot report errors without help. See
//     installEmscriptenEhShim() below.
// ---------------------------------------------------------------------------

import { createRequire } from 'node:module';
import { fileURLToPath } from 'node:url';
import path from 'node:path';
import fs from 'node:fs';

const require = createRequire(import.meta.url);
const HERE = path.dirname(fileURLToPath(import.meta.url));
const REPO = path.resolve(HERE, '..', '..');

const EXT_WASM =
  process.env.CITYJSON_WASM_EXT ||
  path.join(REPO, 'build/wasm_mvp/extension/cityjson/cityjson.duckdb_extension.wasm');

// Scratch HOME for the extension cache (see seedExtensionCache). Kept inside
// test/wasm/ and gitignored so a run never touches the developer's real ~/.duckdb.
const CACHE_HOME = path.join(HERE, '.cache-home');

// --------------------------------------------------------------------------
// Trap 3: the wasm_mvp bundle cannot report errors without this shim.
//
// duckdb-mvp.wasm is built with *JS* exception handling: it has 343 `invoke_*`
// imports, and the JS glue's `invoke_*` wrappers call `_setThrew(1, 0)` on catch,
// while `___cxa_find_matching_catch_*` calls `___cxa_can_catch`. Neither of those
// JS bindings is ever *defined* in the shipped bundle -- `_setThrew` is referenced
// 345 times in `duckdb-node-blocking.cjs` and assigned zero times (same in
// `duckdb-node-mvp.worker.cjs`, and same in the older 1.32.0 release). So the first
// C++ exception DuckDB throws surfaces as `ReferenceError: _setThrew is not
// defined` instead of the actual message. Reproduces on a stock instance with no
// extension loaded at all:
//
//   SELECT * FROM no_such_table;
//   -- expected: Catalog Error: Table with name no_such_table does not exist!
//   -- actual:   ReferenceError: _setThrew is not defined
//
// The `duckdb-eh.wasm` bundle is unaffected (native wasm exceptions, 0 `invoke_*`
// imports), which is why this is not a universal duckdb-wasm problem.
//
// The references are free variables, so they resolve against globalThis -- and the
// functions they want *are* exported by the wasm module (`setThrew`, `__cxa_*`).
// Binding them restores exactly the upstream-intended behaviour rather than
// papering over it: `setThrew(1, 0)` is emscripten's own implementation, called
// with emscripten's own arguments. We capture the exports by wrapping the two
// instantiation entry points, since the glue keeps `wasmExports` closure-private.
// --------------------------------------------------------------------------
const EH_GLOBALS = /^(setThrew|__cxa_[A-Za-z0-9_]*|__resumeException|malloc|free)$/;

function installEmscriptenEhShim() {
  let bound = 0;
  const bind = (instance) => {
    const exports = instance && instance.exports;
    // Only the DuckDB main module matters; side modules do not export setThrew.
    if (!exports || typeof exports.setThrew !== 'function') return;
    for (const [name, value] of Object.entries(exports)) {
      if (typeof value !== 'function') continue;
      if (!EH_GLOBALS.test(name)) continue;
      if (globalThis['_' + name] !== undefined) continue;
      globalThis['_' + name] = value;
      bound++;
    }
  };

  const originalInstantiate = WebAssembly.instantiate;
  WebAssembly.instantiate = function (...args) {
    return Promise.resolve(originalInstantiate.apply(this, args)).then((result) => {
      bind(result && result.instance ? result.instance : result);
      return result;
    });
  };
  WebAssembly.Instance = new Proxy(WebAssembly.Instance, {
    construct(target, args) {
      const instance = new target(...args);
      bind(instance);
      return instance;
    },
  });
  return () => bound;
}

// --------------------------------------------------------------------------
// Trap 2: how DuckDB-Wasm actually loads an extension under Node.
//
// `LOAD '<path>'` does NOT read the path through DuckDB's filesystem, so
// registering the .wasm with `db.registerFileBuffer()` does nothing. DuckDB-Wasm
// routes every extension load through one EM_JS helper that treats the LOAD
// argument as a URL and, when `XMLHttpRequest` is undefined (i.e. under Node),
// looks for a cached copy at
//
//   os.homedir()/.duckdb/extensions/<seg[-4]>/<seg[-3]>/<seg[-2]>/<seg[-1]>
//
// where `seg` is the LOAD argument split on '/'. Only on a cache miss does it
// fetch, and that fetch path is a trap of its own: it spawns a worker_threads
// worker and blocks on `Atomics.wait`, which is never notified if the fetch
// rejects. A miss therefore *hangs the process forever* rather than erroring --
// which is what `LOAD 'anything-not-cached'` does under Node, including a plain
// nonexistent name.
//
// So: seed the cache slot the loader will look in, and it never fetches. We point
// HOME at a scratch directory first so this stays inside test/wasm/.
// --------------------------------------------------------------------------
function seedExtensionCache(extPath) {
  process.env.HOME = CACHE_HOME;
  const seg = extPath.split('/');
  const dir = path.join(CACHE_HOME, '.duckdb/extensions', seg.at(-4), seg.at(-3), seg.at(-2));
  fs.mkdirSync(dir, { recursive: true });
  fs.copyFileSync(extPath, path.join(dir, seg.at(-1)));
  return path.join(dir, seg.at(-1));
}

// --------------------------------------------------------------------------
// Assertions
// --------------------------------------------------------------------------
let failures = 0;
let passes = 0;

function check(name, actual, expected) {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    console.log(`PASS  ${name}  ${a}`);
    passes++;
  } else {
    console.log(`FAIL  ${name}\n        expected ${e}\n        actual   ${a}`);
    failures++;
  }
}

function fail(name, err) {
  const message = err && err.message ? err.message : String(err);
  console.log(`FAIL  ${name}\n        ${message.split('\n').join('\n        ')}`);
  failures++;
}

// --------------------------------------------------------------------------
// Main
// --------------------------------------------------------------------------
async function main() {
  if (!fs.existsSync(EXT_WASM)) {
    console.error(`missing wasm extension: ${EXT_WASM}\nBuild it first with: just wasm`);
    process.exit(2);
  }

  const boundCount = installEmscriptenEhShim();
  const cached = seedExtensionCache(EXT_WASM);

  const duckdb = require('@duckdb/duckdb-wasm/blocking');
  const DIST = path.dirname(require.resolve('@duckdb/duckdb-wasm'));

  // The artifact is a wasm_mvp build, so offer duckdb-wasm only the mvp bundle --
  // given the choice, selectBundle() picks eh on Node and `pragma_platform()` then
  // reports wasm_eh, which cannot load a wasm_mvp extension.
  const bundles = {
    mvp: { mainModule: path.resolve(DIST, 'duckdb-mvp.wasm'), mainWorker: null },
  };

  console.log(`# @duckdb/duckdb-wasm ${duckdb.PACKAGE_VERSION}`);
  console.log(`# extension           ${EXT_WASM}`);
  console.log(`# cache slot          ${cached}`);

  const db = await duckdb.createDuckDB(bundles, new duckdb.VoidLogger(), duckdb.NODE_RUNTIME);
  await db.instantiate(() => {});
  console.log(`# duckdb              ${db.getVersion()}`);
  console.log(`# emscripten EH shim  bound ${boundCount()} globals`);

  // allowUnsignedExtensions is required: our .wasm is not signed by DuckDB Labs.
  db.open({ path: ':memory:', allowUnsignedExtensions: true });
  const conn = db.connect();

  // Arrow rows carry BigInt; normalise so the JSON compare against the oracle works.
  const query = (sql) =>
    conn
      .query(sql)
      .toArray()
      .map((row) => JSON.parse(JSON.stringify(row.toJSON(), (k, v) => (typeof v === 'bigint' ? Number(v) : v))));

  check('platform is wasm_mvp', query('SELECT * FROM pragma_platform()'), [{ platform: 'wasm_mvp' }]);

  // Guard: if this reports a ReferenceError rather than a Catalog Error, the EH
  // shim above stopped working and every other failure message is untrustworthy.
  try {
    query('SELECT * FROM no_such_table_in_smoke_test');
    check('errors are reported as DuckDB errors', 'no error raised', 'Catalog Error');
  } catch (e) {
    const kind = e instanceof ReferenceError ? 'ReferenceError' : 'Catalog Error';
    check('errors are reported as DuckDB errors', kind, 'Catalog Error');
  }

  try {
    query(`LOAD '${EXT_WASM}'`);
    console.log(`PASS  extension loads`);
    passes++;
  } catch (e) {
    fail('extension loads', e);
    report();
    return;
  }

  // Oracle: ./build/release/duckdb -c \
  //   "SELECT count(*) FROM read_cityjson('test/data/minimal.city.json')"  ->  1
  try {
    check(
      'read_cityjson(minimal.city.json) row count',
      query(`SELECT count(*)::INT AS n FROM read_cityjson('${REPO}/test/data/minimal.city.json')`),
      [{ n: 1 }]
    );
  } catch (e) {
    fail('read_cityjson(minimal.city.json) row count', e);
  }

  // Oracle: ./build/release/duckdb -c \
  //   "SELECT count(*) AS n, min(height) AS min_h FROM read_flatcitybuf('test/data/fcb_bbox_attr.fcb')"
  //   ->  n = 3, min_h = 10.0
  try {
    check(
      'read_flatcitybuf(fcb_bbox_attr.fcb) count + min(height)',
      query(
        `SELECT count(*)::INT AS n, min(height) AS min_h FROM read_flatcitybuf('${REPO}/test/data/fcb_bbox_attr.fcb')`
      ),
      [{ n: 3, min_h: 10.0 }]
    );
  } catch (e) {
    fail('read_flatcitybuf(fcb_bbox_attr.fcb) count + min(height)', e);
  }

  // Network-gated, same opt-in as test/sql/cityjson_fcb_remote.test. The bbox is the
  // 500 m square that test uses, valid only for the default hosted 3DBAG subset
  // (RD New / EPSG:7415) -- a different URL needs a bbox inside its own extent.
  //
  // Expected to XFAIL under Node, and the reason is worth stating precisely because
  // it is *not* our code and *not* the httpfs autoload. DuckDB-Wasm's Node runtime
  // implements exactly one data protocol -- NODE_FS. Its openFile() switch answers
  // BROWSER_FILEREADER / BROWSER_FSACCESS / HTTP / S3 with failWith("Unsupported
  // data protocol"), which reaches SQL as an opaque numeric error code. Verified
  // independently of this extension: plain `read_text('https://...')` on a stock
  // duckdb-wasm instance returns zero rows rather than the file.
  //
  // DuckDBRangeReader's own AutoLoadExtension(context, "httpfs") is NOT the cause and
  // needs no guard: httpfs stays loaded = false, installed = false across the call, no
  // AutoloadException is raised, and the failure arrives from the FileSystem::OpenFile
  // that follows. The browser runtime does implement HTTP (synchronous XHR range
  // requests), so the same artifact may well do remote reads in a browser -- Node just
  // cannot be the place we prove it.
  //
  // Classified rather than skipped, so that the day duckdb-wasm's Node runtime learns
  // HTTP this turns into a PASS on its own, and so that any *other* failure is still a
  // hard failure.
  const NODE_VFS_NO_HTTP = /Opening file '.*' failed with error: \d+/;
  const remoteUrl = process.env.FCB_REMOTE_TEST_URL;
  if (!remoteUrl) {
    console.log('SKIP  remote bbox read (set FCB_REMOTE_TEST_URL to enable)');
  } else {
    try {
      check(
        'remote bbox read returns identified rows',
        query(
          `SELECT (count(*) > 0 AND count(*) = count(id)) AS ok FROM read_flatcitybuf('${remoteUrl}',
             min_x := 84000, min_y := 444000, max_x := 84500, max_y := 444500)`
        ),
        [{ ok: true }]
      );
    } catch (e) {
      const message = e && e.message ? e.message : String(e);
      if (NODE_VFS_NO_HTTP.test(message)) {
        console.log(
          'XFAIL remote bbox read returns identified rows\n' +
            '        duckdb-wasm\'s Node runtime has no HTTP VFS (openFile supports NODE_FS only)\n' +
            `        ${message.split('\n')[0]}`
        );
      } else {
        fail('remote bbox read returns identified rows', e);
      }
    }
  }

  report();
}

function report() {
  console.log(`\n${passes} passed, ${failures} failed`);
  process.exit(failures === 0 ? 0 : 1);
}

main().catch((e) => {
  console.error(e && e.stack ? e.stack : String(e));
  process.exit(1);
});

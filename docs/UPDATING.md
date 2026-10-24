# Extension updating

The extension targets a specific stable DuckDB release. Moving it to a newer one goes as
follows:

- Bump submodules
  - `./duckdb` to the release tag (`v2.0.0`).
  - `./extension-ci-tools` to the branch matching that release. Branches are named for
    the release line and its codename, not the full version: DuckDB `v2.0.0` is
    `v2.0-cyanoptera`.
- Bump `.github/workflows/MainDistributionPipeline.yml`
  - the `@ref` of both reusable workflows and their `ci_tools_version` input to that
    branch, and their `duckdb_version` input to the release tag;
  - in the `tidy-check` job, the extension-ci-tools checkout `ref` to the same branch and
    `vcpkgGitCommitId` to the `vcpkg_commit` default in that branch's
    `_extension_distribution.yml`.
- Bump `vcpkg.json`'s `builtin-baseline` to that same vcpkg commit, so a local build and
  CI resolve the same port versions.
- Bump the `justfile`'s `emsdk_version` to the version `_extension_distribution.yml`
  passes to `setup-emsdk`, and `vcpkg_baseline` to the vcpkg commit.
- Re-pin `test/wasm/package.json` to the `@duckdb/duckdb-wasm` release that embeds the
  new DuckDB, once one exists: the extension's version footer must match.

# API changes
DuckDB extensions built with this extension template are built against the internal C++ API of DuckDB. This API is not guaranteed to be stable.
What this means for extension development is that when updating your extensions DuckDB target version using the above steps, you may run into the fact that your extension no longer builds properly.

Currently, DuckDB does not (yet) provide a specific change log for these API changes, but it is generally not too hard to figure out what has changed.

For figuring out how and why the C++ API changed, we recommend using the following resources:
- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ Header file of the API that has changed
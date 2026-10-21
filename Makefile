PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=cityjson
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# DuckDB derives its version from duckdb/scripts/ci/release_version.txt and a commit
# count, so a build of a release tag reports v2.0.0-dev<N> unless DUCKDB_VERSION names
# the release. An extension built that way carries a dev version footer, which a
# released DuckDB refuses to load, and no extensions are published for it. CI sets
# DUCKDB_VERSION; this does the same locally whenever the submodule sits on a tag.
DUCKDB_VERSION ?= $(shell git -C $(PROJ_DIR)duckdb describe --tags --exact-match 2>/dev/null)
export DUCKDB_VERSION

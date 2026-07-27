#pragma once

#include "cityjson/cityjson_types.hpp"
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * The columns `cityjson_geometry_templates(path)` emits for a given set of templates.
 *
 * Unlike the materials and textures sidecars, whose columns are fixed, this one carries
 * per-LoD geometry and appearance columns and so its schema is a property of the file.
 * A caller that has to match that schema — `insert_cityjson`, evolving a destination
 * sidecar before inserting into it — must ask this rather than reconstruct it.
 *
 * Throws when a template's `lod` is absent or non-numeric: the LoD names the columns,
 * so `geometry_lod` or `geometry_lodfoo` would be unreadable by any conforming reader.
 */
void GeometryTemplateColumns(const GeometryTemplates &templates, std::vector<std::string> &names,
                             std::vector<LogicalType> &types, std::vector<std::string> &lods);

/**
 * Registers the CityParquet appearance sidecar readers:
 *
 *   cityjson_materials(path) -> materials.parquet rows
 *   cityjson_textures(path)  -> textures.parquet rows
 *
 * Column names and order follow the specification's sidecar tables exactly, including
 * its mixed casing (`ambientIntensity`, `wrapMode`, `borderColor`) — those are the
 * spec's names, not a style choice, and a package is read by matching them.
 *
 * `id` is the entry's ordinal position in the source's `appearance.materials` /
 * `appearance.textures` array. That is deterministic, needs no interning, and is the
 * rule `cityparquet-rs` must also follow for packages produced by the two
 * implementations to merge.
 */
void RegisterAppearanceTableFunctions(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

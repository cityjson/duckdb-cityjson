#pragma once

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {
namespace cityjson {

/**
 * Registers cityjson_appearance_ids(cell VARCHAR, kind VARCHAR) -> BIGINT[].
 *
 * Extracts the material or texture ids a `material_lod*` / `texture_lod*` cell
 * references, across every theme in the cell. Used to find sidecar rows nothing
 * references.
 *
 * This is a C++ function rather than generated `json_extract` SQL for two reasons: the
 * JSON type and its functions live in the `json` extension, which this extension does
 * not require; and the two cell shapes genuinely differ —
 *
 *   material: {"<theme>": {"values": [id|null, ...]}}   -- one id per face
 *             {"<theme>": {"value": id}}                -- one id for the geometry
 *   texture:  {"<theme>": {"values": [[[id, uv...], ...], ...]}}
 *                                                       -- per face, per ring;
 *                                                          only each ring's FIRST
 *                                                          element is an id, the rest
 *                                                          are UV references
 *
 * A path expression that treated the texture nesting like the material one would
 * collect UV indices as though they were texture ids.
 */
void RegisterAppearanceIdsFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

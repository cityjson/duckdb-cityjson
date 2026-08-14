#pragma once

#ifdef CITYJSON_HAS_FCB

#include "cityjson/cityjson_types.hpp"
#include <fcb/feature.hpp>
#include <fcb/header.hpp>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * Which parts of a FlatCityBuf feature a scan actually needs.
 *
 * The default is "everything", so a caller that never sets a mask gets exactly
 * today's behaviour. Task 5 computes this from the projected column ids.
 */
struct FcbFieldMask {
	/// Decode geometry at all. False selects the light path below.
	bool geometry = true;
	/// Attribute names to decode; std::nullopt means "all of them".
	std::optional<std::set<std::string>> attributes;
};

/**
 * Build a CityJSONFeature from a FlatCityBuf feature WITHOUT touching geometry.
 *
 * Mirrors fcb::to_cityjson_feature (src/cpp/src/cityjson.cpp) field for field,
 * minus geometry, geometry instances, the feature vertex pool and appearance --
 * all four of which exist only to serve geometry. Everything else (object ids,
 * type / extension_type, the per-object-else-header attribute schema selection,
 * geographicalExtent, children, parents) is reproduced exactly, so a consumer
 * cannot tell a light-path feature from a full-path one with its geometry
 * removed.
 *
 * `mask.geometry` is expected to be false; the field is not read here (the
 * caller decides which path to take), it is only `mask.attributes` that matters.
 *
 * Throws CityJSONError on a malformed attribute blob -- the same posture as the
 * full path, where fcb::decode_attributes's own fcb::Error propagates out.
 */
CityJSONFeature ConvertFeatureLight(const fcb::Feature &feature, const fcb::HeaderView &header,
                                    const FcbFieldMask &mask);

/**
 * Walk a feature's attribute blob, decoding only the columns named in `wanted`
 * (std::nullopt = all of them) and SKIPPING the bytes of the rest without
 * materialising a string, a JSON document or a byte array for them.
 *
 * The output JSON typing is identical to fcb::attributes_to_json's for every
 * column it does decode, so SQL-visible values are the same on both paths.
 *
 * Records are not self-delimiting, so a column index absent from `schema` or a
 * record running past the end of the blob aborts the walk with a CityJSONError
 * -- exactly what fcb::decode_attributes does, and for the same reason: with an
 * unknown width the remainder of the blob cannot be located, masked out or not.
 */
nlohmann::json DecodeAttributesFiltered(const uint8_t *data, size_t size,
                                        const std::vector<fcb::ColumnInfo> &schema,
                                        const std::optional<std::set<std::string>> &wanted);

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB

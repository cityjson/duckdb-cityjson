#pragma once

#include <optional>
#include <string>

namespace duckdb {
namespace cityjson {

// Resolve a CityJSON `metadata.referenceSystem` string to an EPSG code.
// Accepts the OGC URL form (https://www.opengis.net/def/crs/EPSG/0/7415), the
// URN form (urn:ogc:def:crs:EPSG::7415), the short form (EPSG:7415), and a bare
// numeric code (7415). Returns nullopt when no EPSG code can be extracted.
std::optional<int> EpsgCodeFromReferenceSystem(const std::string &reference_system);

// Resolve a CityJSON `metadata.referenceSystem` string to a PROJJSON string.
// Uses the embedded EPSG->PROJJSON table (decompressed and parsed on first use).
// OGC:CRS84 / CRS84 resolves to the WGS 84 lon/lat PROJJSON. Returns nullopt for
// an unknown or unparseable CRS (the caller decides whether that is an error).
std::optional<std::string> ProjjsonForReferenceSystem(const std::string &reference_system);

// Direct EPSG-code lookup into the embedded table. Returns nullopt if absent.
std::optional<std::string> ProjjsonForEpsg(int code);

} // namespace cityjson
} // namespace duckdb

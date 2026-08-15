#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<FunctionData> CityJSONBindData::Copy() const {
	auto result = make_uniq<CityJSONBindData>();
	result->file_name = file_name;
	result->metadata = metadata;
	result->chunks = chunks;
	result->scan_plan = scan_plan;
	result->columns = columns;
	result->target_lod = target_lod;
	result->use_wkb_encoding = use_wkb_encoding;
	result->geometry_encoding = geometry_encoding;
	result->streaming = streaming;
	result->appearance_index = appearance_index;
	result->equality_filters = equality_filters;
	return result;
}

bool CityJSONBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<CityJSONBindData>();
	// appearance_index is derived deterministically from (file_name, appearance
	// option), so presence equality is sufficient; AppearanceIndex itself has no
	// operator==.
	return file_name == other.file_name && target_lod == other.target_lod &&
	       use_wkb_encoding == other.use_wkb_encoding && geometry_encoding == other.geometry_encoding &&
	       streaming == other.streaming &&
	       appearance_index.has_value() == other.appearance_index.has_value() &&
	       equality_filters == other.equality_filters;
}

} // namespace cityjson
} // namespace duckdb

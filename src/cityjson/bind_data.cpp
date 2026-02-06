#include "cityjson/table_function.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<FunctionData> CityJSONBindData::Copy() const {
	auto result = make_uniq<CityJSONBindData>();
	result->file_name = file_name;
	result->metadata = metadata;
	result->chunks = chunks;
	result->columns = columns;
	return std::move(result);
}

bool CityJSONBindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<CityJSONBindData>();
	return file_name == other.file_name;
}

} // namespace cityjson
} // namespace duckdb

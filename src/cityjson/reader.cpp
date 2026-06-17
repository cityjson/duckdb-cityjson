#include "cityjson/reader.hpp"

namespace duckdb {
namespace cityjson {

size_t CityJSONReader::CountCityObjects() const {
	size_t count = 0;
	auto chunks = ReadAllChunks();
	for (size_t i = 0; i < chunks.ChunkCount(); i++) {
		auto chunk = chunks.GetChunk(i);
		if (chunk) {
			for (const auto &feature : *chunk) {
				count += feature.CityObjectCount();
			}
		}
	}
	return count;
}

} // namespace cityjson
} // namespace duckdb

#pragma once

#ifdef CITYJSON_HAS_FCB

#include <fcb/range_reader.hpp>
#include <cstdint>
#include <memory>
#include <string>

namespace duckdb {
class ClientContext;
class FileHandle;

namespace cityjson {

/**
 * fcb::RangeReader backed by DuckDB's own FileSystem, so local paths and any
 * http(s)/s3/gcs URL httpfs already supports are read the same way
 * read_cityjson/read_cityjsonseq read them -- one HTTP stack, one
 * credentials/secrets/proxy story. Auto-loads the httpfs extension for
 * remote paths, matching json_utils::ReadFileContent's own behavior.
 */
class DuckDBRangeReader : public fcb::RangeReader {
public:
	DuckDBRangeReader(duckdb::ClientContext &context, const std::string &path);
	~DuckDBRangeReader() override;

	std::uint64_t total_size() override;
	std::vector<std::uint8_t> read(std::uint64_t offset, std::uint64_t length) override;

private:
	std::string path_;
	std::unique_ptr<duckdb::FileHandle> handle_;
};

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB

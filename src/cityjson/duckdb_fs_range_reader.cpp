#ifdef CITYJSON_HAS_FCB

#include "cityjson/duckdb_fs_range_reader.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {
namespace cityjson {

static bool IsRemotePath(const std::string &path) {
	return path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0 || path.rfind("s3://", 0) == 0 ||
	       path.rfind("s3a://", 0) == 0 || path.rfind("s3n://", 0) == 0 || path.rfind("gcs://", 0) == 0 ||
	       path.rfind("gs://", 0) == 0 || path.rfind("r2://", 0) == 0 || path.rfind("hf://", 0) == 0;
}

DuckDBRangeReader::DuckDBRangeReader(duckdb::ClientContext &context, const std::string &path) : path_(path) {
	if (IsRemotePath(path_)) {
		duckdb::ExtensionHelper::AutoLoadExtension(context, "httpfs");
	}
	auto &fs = duckdb::FileSystem::GetFileSystem(context);
	try {
		handle_ = fs.OpenFile(path_, duckdb::FileOpenFlags::FILE_FLAGS_READ);
	} catch (const std::exception &e) {
		throw fcb::Error(fcb::ErrorCode::IoError, "Failed to open " + path_ + ": " + e.what());
	}
	if (!handle_) {
		throw fcb::Error(fcb::ErrorCode::IoError, "Failed to open " + path_);
	}
}

DuckDBRangeReader::~DuckDBRangeReader() = default;

std::uint64_t DuckDBRangeReader::total_size() {
	return static_cast<std::uint64_t>(handle_->GetFileSize());
}

std::vector<std::uint8_t> DuckDBRangeReader::read(std::uint64_t offset, std::uint64_t length) {
	auto total = total_size();
	if (offset >= total || length == 0) {
		return {};
	}
	// Contract (fcb/range_reader.hpp): clamp to what actually exists past `offset`
	// rather than throwing -- only a genuinely truncated transport read is an error.
	std::uint64_t clamped_length = std::min<std::uint64_t>(length, total - offset);
	std::vector<std::uint8_t> buffer(clamped_length);
	try {
		handle_->Read(buffer.data(), clamped_length, offset);
	} catch (const std::exception &e) {
		throw fcb::Error(fcb::ErrorCode::IoError,
		                 "Read failed at offset " + std::to_string(offset) + " of " + path_ + ": " + e.what());
	}
	return buffer;
}

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB

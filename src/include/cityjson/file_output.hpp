#pragma once

#include "duckdb/common/file_system.hpp"

#include <memory>
#include <ostream>
#include <streambuf>
#include <string>
#include <vector>

namespace duckdb {
namespace cityjson {

/**
 * A std::streambuf that buffers bytes and hands them to a DuckDB FileHandle.
 *
 * Writers produce their output through std::ostream (nlohmann::json, the FCB
 * writer), but the bytes must reach DuckDB's FileSystem rather than the C
 * runtime's: only the FileSystem honours `enable_external_access` /
 * `allowed_directories`, and under DuckDB-Wasm only the FileSystem is the file
 * system the host can read back -- a std::ofstream lands in Emscripten's MEMFS,
 * which DuckDB never sees.
 *
 * A failed FileSystem write cannot propagate through std::ostream (it would be
 * swallowed into badbit), so the first error is kept and FileOutput::Close()
 * rethrows it.
 */
class FileHandleStreamBuf : public std::streambuf {
public:
	explicit FileHandleStreamBuf(FileHandle &handle, std::size_t buffer_size = 1U << 20U);

	//! The message of the first failed write, or empty.
	const std::string &Error() const {
		return error_;
	}

protected:
	int_type overflow(int_type ch) override;
	std::streamsize xsputn(const char *data, std::streamsize count) override;
	int sync() override;

private:
	bool Flush();

	FileHandle &handle_;
	std::vector<char> buffer_;
	std::string error_;
};

/**
 * A file opened for writing through DuckDB's FileSystem, exposed as std::ostream.
 * Creates the file, truncating any existing one. Call Close() to flush and to
 * surface any write error; destruction without Close() discards errors.
 */
class FileOutput {
public:
	FileOutput(FileSystem &fs, const std::string &path);
	~FileOutput();

	FileOutput(const FileOutput &) = delete;
	FileOutput &operator=(const FileOutput &) = delete;

	std::ostream &Stream() {
		return *stream_;
	}

	//! Flush, close the handle, and throw CityJSONError::FileWrite if any write failed.
	void Close();

private:
	std::string path_;
	unique_ptr<FileHandle> handle_;
	std::unique_ptr<FileHandleStreamBuf> buf_;
	std::unique_ptr<std::ostream> stream_;
};

} // namespace cityjson
} // namespace duckdb

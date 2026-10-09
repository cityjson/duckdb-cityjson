#include "cityjson/file_output.hpp"
#include "cityjson/error.hpp"

#include <exception>

namespace duckdb {
namespace cityjson {

// ============================================================
// FileHandleStreamBuf
// ============================================================

FileHandleStreamBuf::FileHandleStreamBuf(FileHandle &handle, std::size_t buffer_size)
    : handle_(handle), buffer_(buffer_size) {
	setp(buffer_.data(), buffer_.data() + buffer_.size());
}

bool FileHandleStreamBuf::Flush() {
	const auto pending = pptr() - pbase();
	if (pending == 0) {
		return true;
	}
	if (!error_.empty()) {
		return false;
	}
	try {
		handle_.Write(pbase(), static_cast<idx_t>(pending));
	} catch (const std::exception &e) {
		error_ = e.what();
		return false;
	}
	setp(buffer_.data(), buffer_.data() + buffer_.size());
	return true;
}

FileHandleStreamBuf::int_type FileHandleStreamBuf::overflow(int_type ch) {
	if (!Flush()) {
		return traits_type::eof();
	}
	if (!traits_type::eq_int_type(ch, traits_type::eof())) {
		*pptr() = traits_type::to_char_type(ch);
		pbump(1);
	}
	return traits_type::not_eof(ch);
}

std::streamsize FileHandleStreamBuf::xsputn(const char *data, std::streamsize count) {
	std::streamsize written = 0;
	while (written < count) {
		const auto room = epptr() - pptr();
		if (room == 0) {
			if (!Flush()) {
				return written;
			}
			continue;
		}
		const auto chunk = std::min<std::streamsize>(room, count - written);
		traits_type::copy(pptr(), data + written, static_cast<std::size_t>(chunk));
		pbump(static_cast<int>(chunk));
		written += chunk;
	}
	return written;
}

int FileHandleStreamBuf::sync() {
	return Flush() ? 0 : -1;
}

// ============================================================
// FileOutput
// ============================================================

FileOutput::FileOutput(FileSystem &fs, const std::string &path) : path_(path) {
	try {
		handle_ = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);
	} catch (const std::exception &e) {
		throw CityJSONError::FileWrite("Failed to open output file: " + path + ": " + e.what());
	}
	if (!handle_) {
		throw CityJSONError::FileWrite("Failed to open output file: " + path);
	}
	buf_ = std::make_unique<FileHandleStreamBuf>(*handle_);
	stream_ = std::make_unique<std::ostream>(buf_.get());
}

FileOutput::~FileOutput() {
	if (!handle_) {
		return;
	}
	try {
		stream_->flush();
		handle_->Close();
	} catch (...) { // NOLINT(bugprone-empty-catch): a destructor must not throw; Close() reports errors
	}
}

void FileOutput::Close() {
	stream_->flush();
	const auto error = buf_->Error();
	const bool failed = !error.empty() || !*stream_;
	try {
		handle_->Close();
	} catch (const std::exception &e) {
		handle_.reset();
		throw CityJSONError::FileWrite("Failed writing output file: " + path_ + ": " + e.what());
	}
	handle_.reset();
	if (failed) {
		throw CityJSONError::FileWrite("Failed writing output file: " + path_ + (error.empty() ? "" : ": " + error));
	}
}

} // namespace cityjson
} // namespace duckdb

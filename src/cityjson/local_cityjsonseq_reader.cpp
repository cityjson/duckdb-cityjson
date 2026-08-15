#include "cityjson/reader.hpp"
#include "cityjson/city_object_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension_helper.hpp"

namespace duckdb {
namespace cityjson {

using namespace json_utils; // NOLINT(google-build-using-namespace)

// ============================================================
// Helpers
// ============================================================

static bool IsRemoteFile(const std::string &path) {
	return path.rfind("http://", 0) == 0 || path.rfind("https://", 0) == 0 || path.rfind("s3://", 0) == 0 ||
	       path.rfind("s3a://", 0) == 0 || path.rfind("s3n://", 0) == 0 || path.rfind("gcs://", 0) == 0 ||
	       path.rfind("gs://", 0) == 0 || path.rfind("r2://", 0) == 0 || path.rfind("hf://", 0) == 0;
}

// ============================================================
// Constructors
// ============================================================

LocalCityJSONSeqReader::LocalCityJSONSeqReader(duckdb::ClientContext &context, const std::string &file_path,
                                               size_t sample_lines)
    : file_path_(file_path), sample_lines_(sample_lines), context_(context) {
	OpenHandle();
}

// ============================================================
// Name
// ============================================================

std::string LocalCityJSONSeqReader::Name() const {
	return file_path_;
}

// ============================================================
// FileHandle management
// ============================================================

void LocalCityJSONSeqReader::OpenHandle() const {
	if (IsRemoteFile(file_path_)) {
		duckdb::ExtensionHelper::AutoLoadExtension(context_, "httpfs");
	}
	auto &fs = duckdb::FileSystem::GetFileSystem(context_);
	handle_ = fs.OpenFile(file_path_, duckdb::FileOpenFlags::FILE_FLAGS_READ);
	if (!handle_) {
		throw CityJSONError::FileRead("Failed to open file: " + file_path_);
	}
	metadata_read_ = false;
}

void LocalCityJSONSeqReader::Rewind() const {
	OpenHandle();
	// The header line has to be consumed again, not merely re-parsed. ReadMetadata()
	// short-circuits on the cache and leaves the handle where it is, so without this the
	// next ReadLine() would hand the header to CityJSONFeature::FromJson.
	handle_->ReadLine();
	metadata_read_ = true;
}

// ============================================================
// ReadMetadata
// ============================================================

CityJSON LocalCityJSONSeqReader::ReadMetadata() const {
	if (cached_metadata_.has_value()) {
		return cached_metadata_.value();
	}

	if (!handle_) {
		OpenHandle();
	}

	// Read first line (metadata record)
	std::string line = handle_->ReadLine();
	if (line.empty()) {
		throw CityJSONError::Sequence("CityJSONSeq file is empty");
	}

	json obj;
	try {
		obj = ParseJson(line);
	} catch (const std::exception &) {
		// A pretty-printed regular CityJSON document has a bare "{" first line that does
		// not parse on its own. Reject it as non-sequence input rather than letting the
		// scan return zero rows.
		throw CityJSONError::Sequence("First line must be CityJSON metadata: the file does not look like "
		                              "CityJSONSeq. Use read_cityjson for regular .city.json files.");
	}
	if (!obj.contains("type") || obj["type"] != "CityJSON") {
		throw CityJSONError::Sequence("First line must be CityJSON metadata");
	}
	// A CityJSONSeq header carries an empty (or absent) CityObjects map; the features live
	// on the following lines. A full CityJSON document instead carries its objects inline on
	// this same line. Reject the latter so a minified .city.json fed to read_cityjsonseq
	// surfaces as a format error instead of a silent empty result.
	if (obj.contains("CityObjects") && obj["CityObjects"].is_object() && !obj["CityObjects"].empty()) {
		throw CityJSONError::Sequence("First line must be CityJSONSeq metadata, not a full CityJSON document "
		                              "(found a non-empty CityObjects). Use read_cityjson for .city.json files.");
	}

	CityJSON metadata = CityJSON::FromJson(obj);
	cached_metadata_ = metadata;
	metadata_read_ = true;
	return metadata;
}

// ============================================================
// ReadNextFeature
// ============================================================

std::optional<CityJSONFeature> LocalCityJSONSeqReader::ReadNextFeature() const {
	if (!handle_) {
		OpenHandle();
	}

	// Metadata must be consumed before feature lines
	if (!metadata_read_) {
		ReadMetadata();
	}

	std::string line;
	while (true) {
		try {
			line = handle_->ReadLine();
		} catch (const duckdb::IOException &) {
			// End of file
			return std::nullopt;
		}
		if (line.empty()) {
			// Skip empty lines, but also detect EOF when ReadLine returns empty
			if (handle_->GetFileSize() == handle_->SeekPosition()) {
				return std::nullopt;
			}
			continue;
		}
		break;
	}

	try {
		json feature_obj = ParseJson(line);
		return CityJSONFeature::FromJson(feature_obj);
	} catch (const CityJSONError &e) {
		throw CityJSONError::Sequence("Failed to parse feature: " + std::string(e.what()), file_path_);
	}
}

// ============================================================
// ReadNFeatures
// ============================================================

std::vector<CityJSONFeature> LocalCityJSONSeqReader::ReadNFeatures(size_t n) const {
	std::vector<CityJSONFeature> features;
	features.reserve(n);

	// "The first n features", not "the next n". A sample must start at the start, and a
	// caller that has already read the file gets the same sample as one that has not.
	// Without this, inferring the schema after any other whole-file read sampled an
	// exhausted stream and produced a schema with neither geometry nor attributes.
	// ReadNextFeature is the incremental cursor and is deliberately left alone -- the
	// streaming scan opens a reader of its own (init_global.cpp) and is unaffected.
	Rewind();

	for (size_t i = 0; i < n; i++) {
		auto feature = ReadNextFeature();
		if (!feature.has_value()) {
			break;
		}
		features.push_back(std::move(feature.value()));
	}

	return features;
}

// ============================================================
// ReadAllChunks
// ============================================================

CityJSONFeatureChunk LocalCityJSONSeqReader::ReadAllChunks() const {
	std::vector<CityJSONFeature> features;

	// "All chunks" has to mean all of them however often it is asked. The bind reads all
	// chunks and then interns the appearance from all chunks again; the second call used
	// to return nothing, so every feature-local material index fell through to the
	// identity mapping and pointed at whichever definition the header happened to hold
	// at that position.
	Rewind();

	while (true) {
		auto feature = ReadNextFeature();
		if (!feature.has_value()) {
			break;
		}
		features.push_back(std::move(feature.value()));
	}

	return CityJSONFeatureChunk::CreateChunks(std::move(features), STANDARD_VECTOR_SIZE);
}

// ============================================================
// CountCityObjects
// ============================================================

size_t LocalCityJSONSeqReader::CountCityObjects() const {
	if (!handle_) {
		OpenHandle();
	}

	// Consume metadata line if we haven't already
	if (!metadata_read_) {
		ReadMetadata();
	}

	size_t count = 0;
	while (true) {
		std::string line;
		try {
			line = handle_->ReadLine();
		} catch (const duckdb::IOException &) {
			break;
		}
		if (line.empty()) {
			if (handle_->GetFileSize() == handle_->SeekPosition()) {
				break;
			}
			continue;
		}

		try {
			json feature_obj = ParseJson(line);
			if (feature_obj.contains("CityObjects") && feature_obj["CityObjects"].is_object()) {
				count += feature_obj["CityObjects"].size();
			}
		} catch (const CityJSONError &e) {
			throw CityJSONError::Sequence("Failed to parse feature: " + std::string(e.what()), file_path_);
		}
	}

	return count;
}

// ============================================================
// Columns
// ============================================================

std::vector<Column> LocalCityJSONSeqReader::Columns() const {
	if (cached_columns_.has_value()) {
		return cached_columns_.value();
	}

	std::vector<Column> columns = GetDefinedColumns();
	std::vector<CityJSONFeature> sample_features = ReadNFeatures(sample_lines_);
	std::vector<Column> attr_columns = CityObjectUtils::InferAttributeColumns(sample_features, sample_lines_);
	std::vector<Column> geom_columns = CityObjectUtils::InferGeometryColumns(sample_features, sample_lines_);

	columns.insert(columns.end(), attr_columns.begin(), attr_columns.end());
	columns.insert(columns.end(), geom_columns.begin(), geom_columns.end());

	cached_columns_ = columns;
	return columns;
}

} // namespace cityjson
} // namespace duckdb

#include "cityjson/table_function.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/citygml_parser.hpp"
#include "cityjson/json_utils.hpp"
#include "cityjson/lod_table.hpp"
#include "cityjson/column_types.hpp"
#include "duckdb/common/exception.hpp"

namespace duckdb {
namespace cityjson {

unique_ptr<FunctionData> CityJSONBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CityJSONBindData>();

	// Get file_name from first positional parameter
	if (input.inputs.empty()) {
		throw BinderException("read_cityjson requires a file path");
	}
	result->file_name = StringValue::Get(input.inputs[0]);

	// Parse named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "lod") {
			result->target_lod = StringValue::Get(kv.second);
			result->use_wkb_encoding = true; // Enable WKB encoding when LOD is specified
		}
	}

	// Open reader
	std::unique_ptr<CityJSONReader> reader;
	try {
		reader = OpenAnyCityJSONFile(context, result->file_name);
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to open CityJSON file: " + std::string(e.what()));
	}

	// Read metadata
	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read CityJSON metadata: " + std::string(e.what()));
	}

	// Load all data first (needed for schema inference)
	try {
		result->chunks = reader->ReadAllChunks();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read CityJSON data: " + std::string(e.what()));
	}

	// Infer schema - use LOD table schema if LOD is specified
	if (result->target_lod.has_value()) {
		// Get all features from chunks
		std::vector<CityJSONFeature> all_features;
		for (size_t i = 0; i < result->chunks.ChunkCount(); i++) {
			auto chunk = result->chunks.GetChunk(i);
			if (chunk) {
				all_features.insert(all_features.end(), chunk->begin(), chunk->end());
			}
		}

		// Use LODTableUtils to get the per-LOD schema
		auto lod_tables = LODTableUtils::InferLODTables(all_features);

		// Find the table for the requested LOD
		bool found = false;
		for (const auto &table : lod_tables) {
			if (table.lod_value == result->target_lod.value()) {
				result->columns = table.columns;
				found = true;
				break;
			}
		}

		if (!found) {
			throw BinderException("LOD '" + result->target_lod.value() +
			                      "' not found in CityJSON file. Available LODs: " +
			                      (lod_tables.empty() ? "none" : lod_tables[0].lod_value));
		}
	} else {
		// Use traditional column inference (no WKB encoding)
		try {
			result->columns = reader->Columns();
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to infer schema: " + std::string(e.what()));
		}
	}

	// Populate return types and names
	for (const auto &col : result->columns) {
		names.push_back(col.name);
		return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
	}

	return std::move(result);
}

unique_ptr<FunctionData> CityJSONSeqBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CityJSONBindData>();

	// Get file_name from first positional parameter
	if (input.inputs.empty()) {
		throw BinderException("read_cityjsonseq requires a file path");
	}
	result->file_name = StringValue::Get(input.inputs[0]);

	// Parse named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "lod") {
			result->target_lod = StringValue::Get(kv.second);
			result->use_wkb_encoding = true;
		}
	}

	// Read file content via DuckDB FileSystem, then create CityJSONSeq reader
	std::unique_ptr<CityJSONReader> reader;
	try {
		std::string content = json_utils::ReadFileContent(context, result->file_name);
		reader = std::make_unique<LocalCityJSONSeqReader>(result->file_name, std::move(content), 100);
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to open CityJSONSeq file: " + std::string(e.what()));
	}

	// Read metadata (first line of .jsonl file)
	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read CityJSONSeq metadata: " + std::string(e.what()));
	}

	// Load all data (reads from second line onward)
	try {
		result->chunks = reader->ReadAllChunks();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read CityJSONSeq data: " + std::string(e.what()));
	}

	// Infer schema
	if (result->target_lod.has_value()) {
		std::vector<CityJSONFeature> all_features;
		for (size_t i = 0; i < result->chunks.ChunkCount(); i++) {
			auto chunk = result->chunks.GetChunk(i);
			if (chunk) {
				all_features.insert(all_features.end(), chunk->begin(), chunk->end());
			}
		}

		auto lod_tables = LODTableUtils::InferLODTables(all_features);

		bool found = false;
		for (const auto &table : lod_tables) {
			if (table.lod_value == result->target_lod.value()) {
				result->columns = table.columns;
				found = true;
				break;
			}
		}

		if (!found) {
			throw BinderException("LOD '" + result->target_lod.value() +
			                      "' not found in CityJSONSeq file. Available LODs: " +
			                      (lod_tables.empty() ? "none" : lod_tables[0].lod_value));
		}
	} else {
		try {
			result->columns = reader->Columns();
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to infer schema: " + std::string(e.what()));
		}
	}

	// Populate return types and names
	for (const auto &col : result->columns) {
		names.push_back(col.name);
		return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
	}

	return std::move(result);
}

unique_ptr<FunctionData> CityGMLBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<CityJSONBindData>();

	if (input.inputs.empty()) {
		throw BinderException("read_citygml requires a file path");
	}
	result->file_name = StringValue::Get(input.inputs[0]);

	// Parse named parameters
	for (auto &kv : input.named_parameters) {
		if (kv.first == "lod") {
			result->target_lod = StringValue::Get(kv.second);
			result->use_wkb_encoding = true;
		}
	}

	// Read file content via DuckDB FileSystem, create CityGML reader
	std::unique_ptr<CityJSONReader> reader;
	try {
		std::string content = json_utils::ReadFileContent(context, result->file_name);
		reader = std::make_unique<LocalCityGMLReader>(result->file_name, std::move(content), 100);
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to open CityGML file: " + std::string(e.what()));
	}

	// Read metadata
	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read CityGML metadata: " + std::string(e.what()));
	}

	// Load all data
	try {
		result->chunks = reader->ReadAllChunks();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read CityGML data: " + std::string(e.what()));
	}

	// Infer schema
	if (result->target_lod.has_value()) {
		std::vector<CityJSONFeature> all_features;
		for (size_t i = 0; i < result->chunks.ChunkCount(); i++) {
			auto chunk = result->chunks.GetChunk(i);
			if (chunk) {
				all_features.insert(all_features.end(), chunk->begin(), chunk->end());
			}
		}

		auto lod_tables = LODTableUtils::InferLODTables(all_features);

		bool found = false;
		for (const auto &table : lod_tables) {
			if (table.lod_value == result->target_lod.value()) {
				result->columns = table.columns;
				found = true;
				break;
			}
		}

		if (!found) {
			throw BinderException("LOD '" + result->target_lod.value() +
			                      "' not found in CityGML file. Available LODs: " +
			                      (lod_tables.empty() ? "none" : lod_tables[0].lod_value));
		}
	} else {
		try {
			result->columns = reader->Columns();
		} catch (const CityJSONError &e) {
			throw BinderException("Failed to infer schema: " + std::string(e.what()));
		}
	}

	// Populate return types and names
	for (const auto &col : result->columns) {
		names.push_back(col.name);
		return_types.push_back(ColumnTypeUtils::ToDuckDBType(col.kind));
	}

	return std::move(result);
}

} // namespace cityjson
} // namespace duckdb

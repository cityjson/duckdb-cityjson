#include "cityjson/metadata_table_function.hpp"
#include "cityjson/metadata_table.hpp"
#include "cityjson/reader.hpp"
#include "cityjson/citygml_reader.hpp"
#include "cityjson/json_utils.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {
namespace cityjson {

// Bind data for the metadata table function
struct MetadataBindData : public TableFunctionData {
	std::string file_name;
	CityJSON metadata;
	idx_t city_objects_count;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<MetadataBindData>();
		result->file_name = file_name;
		result->metadata = metadata;
		result->city_objects_count = city_objects_count;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		auto &other_data = other.Cast<MetadataBindData>();
		return file_name == other_data.file_name;
	}
};

// Global state for the metadata table function
struct MetadataGlobalState : public GlobalTableFunctionState {
	bool done = false;

	idx_t MaxThreads() const override {
		return 1;
	}
};

// Bind function for cityjson_metadata
static unique_ptr<FunctionData> MetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<MetadataBindData>();

	// Get file path from argument
	result->file_name = StringValue::Get(input.inputs[0]);

	// Create reader using factory function
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

	// Count city objects by reading all chunks
	result->city_objects_count = 0;
	try {
		auto chunks = reader->ReadAllChunks();
		for (size_t i = 0; i < chunks.ChunkCount(); i++) {
			auto chunk = chunks.GetChunk(i);
			if (chunk) {
				for (const auto &feature : *chunk) {
					result->city_objects_count += feature.city_objects.size();
				}
			}
		}
	} catch (const CityJSONError &e) {
		// Silently ignore if we can't count - set to 0
		result->city_objects_count = 0;
	}

	// Set return types and names
	return_types = MetadataTableUtils::GetMetadataTableTypes();
	names = MetadataTableUtils::GetMetadataTableNames();

	return std::move(result);
}

// Init global state
static unique_ptr<GlobalTableFunctionState> MetadataInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<MetadataGlobalState>();
}

// Scan function for cityjson_metadata
static void MetadataScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<MetadataBindData>();
	auto &global_state = data.global_state->Cast<MetadataGlobalState>();

	if (global_state.done) {
		output.SetCardinality(0);
		return;
	}

	// Create the metadata chunk
	auto metadata_chunk = MetadataTableUtils::CreateMetadataChunk(bind_data.metadata, bind_data.city_objects_count);

	// Copy data to output (not Reference, which would dangle after metadata_chunk is destroyed)
	output.SetCardinality(1);
	for (idx_t col = 0; col < metadata_chunk->ColumnCount(); col++) {
		output.data[col].SetValue(0, metadata_chunk->data[col].GetValue(0));
	}

	global_state.done = true;
}

TableFunction CreateMetadataTableFunction() {
	TableFunction func("cityjson_metadata", {LogicalType::VARCHAR}, MetadataScan, MetadataBind);
	func.init_global = MetadataInitGlobal;
	return func;
}

void RegisterMetadataTableFunction(ExtensionLoader &loader) {
	auto func = CreateMetadataTableFunction();
	loader.RegisterFunction(func);
}

// Bind function for cityjsonseq_metadata — always uses LocalCityJSONSeqReader
static unique_ptr<FunctionData> SeqMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<MetadataBindData>();

	// Get file path from argument
	result->file_name = StringValue::Get(input.inputs[0]);

	// Read file via DuckDB FileSystem, then create CityJSONSeq reader
	std::unique_ptr<CityJSONReader> reader;
	try {
		std::string content = json_utils::ReadFileContent(context, result->file_name);
		reader = std::make_unique<LocalCityJSONSeqReader>(result->file_name, std::move(content), 100);
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to open CityJSONSeq file: " + std::string(e.what()));
	}

	// Read metadata from first line
	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read CityJSONSeq metadata: " + std::string(e.what()));
	}

	// Count city objects from second line onwards
	result->city_objects_count = 0;
	try {
		auto chunks = reader->ReadAllChunks();
		for (size_t i = 0; i < chunks.ChunkCount(); i++) {
			auto chunk = chunks.GetChunk(i);
			if (chunk) {
				for (const auto &feature : *chunk) {
					result->city_objects_count += feature.city_objects.size();
				}
			}
		}
	} catch (const CityJSONError &e) {
		// Silently ignore if we can't count - set to 0
		result->city_objects_count = 0;
	}

	// Set return types and names
	return_types = MetadataTableUtils::GetMetadataTableTypes();
	names = MetadataTableUtils::GetMetadataTableNames();

	return std::move(result);
}

TableFunction CreateCityJSONSeqMetadataTableFunction() {
	TableFunction func("cityjsonseq_metadata", {LogicalType::VARCHAR}, MetadataScan, SeqMetadataBind);
	func.init_global = MetadataInitGlobal;
	return func;
}

void RegisterCityJSONSeqMetadataTableFunction(ExtensionLoader &loader) {
	auto func = CreateCityJSONSeqMetadataTableFunction();
	loader.RegisterFunction(func);
}

// Bind function for citygml_metadata — always uses LocalCityGMLReader
static unique_ptr<FunctionData> CityGMLMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<MetadataBindData>();

	result->file_name = StringValue::Get(input.inputs[0]);

	std::unique_ptr<CityJSONReader> reader;
	try {
		std::string content = json_utils::ReadFileContent(context, result->file_name);
		reader = std::make_unique<LocalCityGMLReader>(result->file_name, std::move(content), 100);
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to open CityGML file: " + std::string(e.what()));
	}

	try {
		result->metadata = reader->ReadMetadata();
	} catch (const CityJSONError &e) {
		throw BinderException("Failed to read CityGML metadata: " + std::string(e.what()));
	}

	result->city_objects_count = 0;
	try {
		auto chunks = reader->ReadAllChunks();
		for (size_t i = 0; i < chunks.ChunkCount(); i++) {
			auto chunk = chunks.GetChunk(i);
			if (chunk) {
				for (const auto &feature : *chunk) {
					result->city_objects_count += feature.city_objects.size();
				}
			}
		}
	} catch (const CityJSONError &e) {
		result->city_objects_count = 0;
	}

	return_types = MetadataTableUtils::GetMetadataTableTypes();
	names = MetadataTableUtils::GetMetadataTableNames();

	return std::move(result);
}

TableFunction CreateCityGMLMetadataTableFunction() {
	TableFunction func("citygml_metadata", {LogicalType::VARCHAR}, MetadataScan, CityGMLMetadataBind);
	func.init_global = MetadataInitGlobal;
	return func;
}

void RegisterCityGMLMetadataTableFunction(ExtensionLoader &loader) {
	auto func = CreateCityGMLMetadataTableFunction();
	loader.RegisterFunction(func);
}

} // namespace cityjson
} // namespace duckdb

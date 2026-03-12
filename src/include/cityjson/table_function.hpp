#pragma once

#include "cityjson/types.hpp"
#include "cityjson/cityjson_types.hpp"
#include "cityjson/column_types.hpp"
#include "duckdb.hpp"
#include <string>
#include <vector>
#include <atomic>
#include <memory>

namespace duckdb {

class ExtensionLoader;

namespace cityjson {

// ============================================================
// Bind Data
// ============================================================

/**
 * Bind data for CityJSON table function
 * Contains file information, metadata, chunks, and schema
 */
struct CityJSONBindData : public TableFunctionData {
	std::string file_name;                 // Path to CityJSON file
	CityJSON metadata;                     // CityJSON metadata
	CityJSONFeatureChunk chunks;           // All data divided into chunks
	std::vector<Column> columns;           // Complete column schema
	std::optional<std::string> target_lod; // Optional: filter to specific LOD
	bool use_wkb_encoding = false;         // Use WKB geometry encoding (when lod specified)

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

// ============================================================
// Global State
// ============================================================

/**
 * Global state for parallel scanning
 * Shared across all threads
 */
struct CityJSONGlobalState : public GlobalTableFunctionState {
	std::atomic<size_t> batch_index; // Current batch index for parallel scanning

	CityJSONGlobalState();

	idx_t MaxThreads() const override;
};

// ============================================================
// Local State
// ============================================================

/**
 * Local state for each scanning thread
 * Thread-local storage for projection information
 */
struct CityJSONLocalState : public LocalTableFunctionState {
	vector<column_t> column_ids;  // Column IDs for projection
	vector<idx_t> projection_ids; // Projection indices
};

// ============================================================
// Table Function Callbacks
// ============================================================

/**
 * Bind callback for read_cityjson - schema inference and data loading
 * Uses format auto-detection
 */
unique_ptr<FunctionData> CityJSONBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names);

/**
 * Bind callback for read_cityjsonseq - schema inference and data loading
 * Always uses LocalCityJSONSeqReader (no auto-detection)
 */
unique_ptr<FunctionData> CityJSONSeqBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names);

/**
 * Bind callback for read_citygml - schema inference and data loading
 * Always uses LocalCityGMLReader for CityGML XML files
 */
unique_ptr<FunctionData> CityGMLBind(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names);

/**
 * Init global state callback
 */
unique_ptr<GlobalTableFunctionState> CityJSONInitGlobal(ClientContext &context, TableFunctionInitInput &input);

/**
 * Init local state callback
 */
unique_ptr<LocalTableFunctionState> CityJSONInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                      GlobalTableFunctionState *global_state);

/**
 * Scan function - main data reading logic
 */
void CityJSONScan(ClientContext &context, TableFunctionInput &data, DataChunk &output);

/**
 * Cardinality callback - estimate number of rows
 */
unique_ptr<NodeStatistics> CityJSONCardinality(ClientContext &context, const FunctionData *bind_data_p);

/**
 * Progress callback - track scan progress
 */
double CityJSONProgress(ClientContext &context, const FunctionData *bind_data_p,
                        const GlobalTableFunctionState *global_state_p);

/**
 * Statistics callback - column statistics (optional)
 */
unique_ptr<BaseStatistics> CityJSONStatistics(ClientContext &context, const FunctionData *bind_data_p,
                                              column_t column_index);

// ============================================================
// Registration
// ============================================================

/**
 * Create read_cityjson table function
 */
TableFunction CreateReadCityJSONTableFunction();

/**
 * Register read_cityjson function with database
 */
void RegisterCityJSONTableFunction(ExtensionLoader &loader);

/**
 * Create read_cityjsonseq table function
 * Always reads .jsonl files as CityJSONTextSequence
 */
TableFunction CreateReadCityJSONSeqTableFunction();

/**
 * Register read_cityjsonseq function with database
 */
void RegisterCityJSONSeqTableFunction(ExtensionLoader &loader);

/**
 * Create read_citygml table function
 */
TableFunction CreateReadCityGMLTableFunction();

/**
 * Register read_citygml function with database
 */
void RegisterCityGMLTableFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

#pragma once

#include "cityjson/types.hpp"
#include "cityjson/cityjson_types.hpp"
#include "cityjson/column_types.hpp"
#include "cityjson/reader.hpp"
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
	CityJSONScanPlan scan_plan;            // Precomputed batch -> source position mapping
	std::vector<Column> columns;           // Complete column schema
	std::optional<std::string> target_lod; // Optional: filter to specific LOD
	bool use_wkb_encoding = false;         // Use WKB geometry encoding (when lod specified)
	bool streaming = false;                // True when data is loaded during scan init instead of bind

	// Pushed-down equality filters on scalar columns (column_name -> expected_value)
	std::vector<std::pair<std::string, std::string>> equality_filters;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

/**
 * Read options parsed from named parameters
 */
struct CityJSONReadOptions {
	std::optional<std::string> target_lod;
	bool use_wkb_encoding = false;
	size_t sample_lines = 100;
};

/**
 * Parse named parameters shared by read_cityjson, read_cityjsonseq, and read_flatcitybuf
 */
CityJSONReadOptions ParseCityJSONReadOptions(const TableFunctionBindInput &input, const std::string &function_name);

/**
 * Shared bind implementation for CityJSON readers
 *
 * @param context DuckDB client context
 * @param input Table function bind input
 * @param return_types Output column types
 * @param names Output column names
 * @param function_name Function name for error messages
 * @param reader Opened reader (ownership transferred)
 * @param streaming When true, data is loaded during scan init instead of bind
 * @return Populated CityJSONBindData
 */
unique_ptr<FunctionData> BindCityJSONRead(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names,
                                          const std::string &function_name,
                                          std::unique_ptr<CityJSONReader> reader, bool streaming = false);

// ============================================================
// Global State
// ============================================================

/**
 * Global state for parallel scanning
 * Shared across all threads
 */
struct CityJSONGlobalState : public GlobalTableFunctionState {
	std::atomic<size_t> batch_index;                 // Current batch index for parallel scanning
	CityJSONFeatureChunk chunks;                     // Materialized chunks (non-streaming)
	CityJSONScanPlan scan_plan;                      // Scan plan for materialized chunks
	std::unique_ptr<CityJSONReader> streaming_reader; // Incremental reader (streaming only)
	std::optional<CityJSONFeature> streaming_feature; // Current feature being processed (streaming only)
	std::map<std::string, CityObject>::const_iterator streaming_obj_it; // Position in current feature

	// Sequential source position used when filter pushdown is active
	size_t filter_chunk_idx = 0;
	size_t filter_feature_idx = 0;
	size_t filter_obj_offset = 0;
	bool has_filters = false;

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
 * Pushdown complex filter callback for CityJSON table functions.
 * Consumes simple equality filters on id/feature_id/object_type.
 */
void CityJSONPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                   vector<unique_ptr<Expression>> &filters);

} // namespace cityjson
} // namespace duckdb

#pragma once

#include "cityjson/types.hpp"
#include "cityjson/appearance_normalise.hpp"
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
	GeometryEncoding geometry_encoding = GeometryEncoding::Wkb; // Physical geometry encoding
	bool streaming = false;                // True when data is loaded during scan init instead of bind
	// The factory this bind opened its reader with, and the sampling depth it opened it
	// at. A streaming scan re-opens the file in init_global and must reproduce both, or
	// it can end up reading the same path through a different reader than the one whose
	// schema it was bound against. See ReaderKind (reader.hpp).
	ReaderKind reader_kind = ReaderKind::Auto;
	size_t sample_lines = 100;
	// Set only when appearance := 'sidecar'. Holds the dataset-global material/texture
	// sets and the per-feature index maps that reach them.
	std::optional<AppearanceIndex> appearance_index;

	// Pushed-down equality filters on scalar columns (column_name -> expected_value)
	std::vector<std::pair<std::string, std::string>> equality_filters;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

/**
 * Read options parsed from named parameters
 */
struct CityJSONReadOptions {
	// 'local' (default) keeps the source's feature-local appearance indices verbatim.
	// 'sidecar' rewrites them to dataset-global sidecar ids and inlines texture UVs,
	// which is what the CityParquet encoding requires.
	bool sidecar_appearance = false;
	std::optional<std::string> target_lod;
	bool use_wkb_encoding = false;
	GeometryEncoding geometry_encoding = GeometryEncoding::Wkb;
	size_t sample_lines = 100;
};

/**
 * Parse named parameters shared by read_cityjson, read_cityjsonseq, and read_flatcitybuf
 */
CityJSONReadOptions ParseCityJSONReadOptions(const TableFunctionBindInput &input, const std::string &function_name);

/**
 * The reader's schema inference, as one function.
 *
 * `insert_cityjson` has to know the columns a read of the same file will produce
 * *before* the read runs, in order to generate the ALTERs and the appearance rewrite.
 * Re-deriving that list from the same ingredients does not work: it produced a column
 * set that disagreed with the reader's, and the generated SQL then named a column the
 * staged relation did not have. So there is exactly one implementation, and both the
 * bind and the generator call it.
 */
void InferCityJSONColumns(CityJSONBindData &bind_data, CityJSONReader &reader, size_t sample_lines);

/**
 * Everything `insert_cityjson` must know about a source file at plan time.
 *
 * `object_types` is the **complete** distinct set, not a sample: routing sends each type
 * to its module table, so a type appearing only in the file's tail would otherwise land
 * in no table at all and its rows would be dropped without a word.
 */
struct CityJSONSourceFacts {
	//! Exactly what a read of this file with these options emits.
	std::vector<Column> columns;
	//! Every distinct CityObject type in the file, sorted.
	std::vector<std::string> object_types;
	//! metadata.referenceSystem, when the file declares one.
	std::optional<std::string> reference_system;
	//! Whether the interned sidecars would have any rows at all. A package's `materials`
	//! table must not be created for a file that has none.
	bool has_materials = false;
	bool has_textures = false;
	//! The document's templates, kept whole rather than reduced to a flag: the
	//! geometry_templates sidecar's columns depend on which LoDs they use, and a caller
	//! evolving a destination sidecar has to know them.
	GeometryTemplates geometry_templates;
};

/**
 * Read `reader` far enough to answer everything in CityJSONSourceFacts.
 *
 * `streaming` must match the read function whose output is being described —
 * read_cityjsonseq binds in streaming mode and read_cityjson does not, and with `lod =`
 * the two infer from different amounts of the file. The object types and the appearance
 * are always taken from the whole file regardless, because a sample cannot answer either.
 */
CityJSONSourceFacts InspectCityJSONSource(CityJSONReader &reader, const CityJSONReadOptions &options, bool streaming);

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
 * @param reader_kind Which factory produced `reader`; recorded so a streaming scan can
 *                    re-open the file the same way instead of re-detecting the format
 * @return Populated CityJSONBindData
 */
unique_ptr<FunctionData> BindCityJSONRead(ClientContext &context, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names,
                                          const std::string &function_name,
                                          std::unique_ptr<CityJSONReader> reader, bool streaming = false,
                                          ReaderKind reader_kind = ReaderKind::Auto);

/**
 * Same as BindCityJSONRead, but takes the reader by reference instead of by
 * unique_ptr, for callers (read_flatcitybuf) that need to keep their own
 * ownership handle to the reader after bind completes.
 *
 * `materialise = false` binds the schema without the bind-time `ReadAllChunks()`.
 * It is meaningful only for a non-streaming caller that materialises later itself
 * (read_flatcitybuf, in its init_global, once the projection is known); such a
 * caller MUST supply the chunks and the scan plan through CityJSONGlobalState and
 * set `use_global_chunks`, or the scan will find nothing to read.
 */
CityJSONBindData BindCityJSONReadRaw(ClientContext &context, TableFunctionBindInput &input,
                                     vector<LogicalType> &return_types, vector<string> &names,
                                     const std::string &function_name, CityJSONReader &reader,
                                     bool streaming = false, bool materialise = true);

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
	// When true the scan reads `chunks`/`scan_plan` above instead of the bind data's.
	// Set by read_flatcitybuf's init_global, which is the only place the projection --
	// and therefore how much of each feature needs decoding -- is known.
	bool use_global_chunks = false;
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

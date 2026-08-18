#pragma once

#ifdef CITYJSON_HAS_FCB

#include "cityjson/table_function.hpp"
#include <array>
#include <memory>
#include <optional>

namespace duckdb {

class ExtensionLoader;

namespace cityjson {

class FlatCityBufReader;

/**
 * Bind data for read_flatcitybuf. Extends the generic CityJSONBindData with an
 * optional bbox and a live reference to the reader that produced `chunks`, so
 * FlatCityBufPushdownComplexFilter can re-query it once a WHERE clause is
 * known -- which happens after Bind, during filter pushdown.
 */
struct FlatCityBufBindData : public CityJSONBindData {
	std::optional<std::array<double, 4>> bbox;
	// shared_ptr, not unique_ptr: Copy() just shares it. Safe because nothing
	// mutates it after the pushdown-filter step (see FlatCityBufPushdownComplexFilter).
	std::shared_ptr<FlatCityBufReader> reader;

	unique_ptr<FunctionData> Copy() const override;
	bool Equals(const FunctionData &other) const override;
};

void FlatCityBufPushdownComplexFilter(ClientContext &context, LogicalGet &get, FunctionData *bind_data,
                                      vector<unique_ptr<Expression>> &filters);

/**
 * Register read_flatcitybuf table function
 */
void RegisterFlatCityBufTableFunction(ExtensionLoader &loader);

/**
 * Register flatcitybuf_metadata table function
 */
void RegisterFlatCityBufMetadataTableFunction(ExtensionLoader &loader);

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB

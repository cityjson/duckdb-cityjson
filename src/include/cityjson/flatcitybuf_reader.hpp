#pragma once

#ifdef CITYJSON_HAS_FCB

#include "cityjson/reader.hpp"
#include <fcb/header.hpp>
#include <fcb/key.hpp>
#include <fcb/reader.hpp>
#include <fcb/stree.hpp>
#include <array>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace duckdb {
class ClientContext;

namespace cityjson {

/**
 * Reader for FlatCityBuf (.fcb) via the native flatcitybuf C++ library.
 * Opens a fresh fcb::FcbReader (over a fresh DuckDBRangeReader transport) per
 * call, matching this class's pre-existing per-call-reopen pattern; only
 * ReadMetadata()/Columns() results are cached across calls.
 */
class FlatCityBufReader : public CityJSONReader {
public:
	FlatCityBufReader(duckdb::ClientContext &context, const std::string &name, const std::string &file_path,
	                  size_t sample_lines = 100);

	std::string Name() const override;
	CityJSON ReadMetadata() const override;
	CityJSONFeatureChunk ReadNthChunk(size_t n) const override;
	CityJSONFeatureChunk ReadAllChunks() const override;
	std::vector<CityJSONFeature> ReadNFeatures(size_t n) const override;
	std::vector<Column> Columns() const override;

	/** Restrict reads to features whose 2D bbox intersects [min_x,min_y,max_x,max_y]. */
	void SetBBoxFilter(std::array<double, 4> bbox);

	/** Restrict reads to features matching every AND-combined condition. */
	void SetAttrQueryFilter(fcb::AttrQuery query, bool exact_index_only = false);

	/** Column names that have a B+tree attribute index, for pushdown eligibility checks. */
	std::vector<std::string> IndexedAttributeColumns() const;

	/** On-disk column info by name, for typing a KeyValue during pushdown. std::nullopt if absent. */
	std::optional<fcb::ColumnInfo> FindColumn(const std::string &name) const;

	/** File header, by value -- fcb::HeaderView owns its own backing buffer. */
	fcb::HeaderView Header() const;

private:
	duckdb::ClientContext &context_;
	std::string name_;
	std::string file_path_;
	size_t sample_lines_;

	std::optional<std::array<double, 4>> bbox_;
	std::optional<fcb::AttrQuery> attr_query_;
	bool attr_query_exact_index_only_ = false;

	mutable std::optional<CityJSON> cached_metadata_;
	mutable std::optional<std::vector<Column>> cached_columns_;

	fcb::FcbReader OpenFcbReader() const;
	fcb::FeatureIterator SelectIterator(fcb::FcbReader &reader) const;
	bool MatchesAttrQueryPostFilter(const CityJSONFeature &feature) const;
	std::vector<CityJSONFeature> ParseFeatures(std::optional<size_t> limit) const;
};

} // namespace cityjson
} // namespace duckdb

#endif // CITYJSON_HAS_FCB

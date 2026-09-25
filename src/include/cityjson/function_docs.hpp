#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/catalog/catalog_entry/pragma_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include <utility>

namespace duckdb {
namespace cityjson {

// What duckdb_functions() reports for one function: the only documentation an
// agent on a SQL connection can reach.
struct FunctionDoc {
	// Positional parameters only. Named parameters are appended from the catalog
	// entry; see AppendNamedParameters.
	vector<string> parameters;
	// One sentence.
	string description;
	// A full SELECT for a table function (a bare call is a binder error), a PRAGMA
	// statement for a pragma, and for a scalar a bare expression when its arguments
	// can be literals, else a SELECT over a reader that supplies them.
	string example;
	vector<string> categories;
};

namespace function_docs_detail {

inline FunctionDescription ToDescription(const FunctionDoc &doc) {
	FunctionDescription description;
	// parameter_types stays empty: an empty list matches every overload, ANY included.
	description.parameter_names = doc.parameters;
	description.description = doc.description;
	description.examples = {doc.example};
	description.categories = doc.categories;
	return description;
}

// duckdb_functions() lists a table or pragma function's named parameters after its
// positional ones, and a description's parameter_names replace that whole list. It
// reads them from a by-value copy of the registered function (GetFunctionByOffset),
// in that copy's named_parameters iteration order -- and copying an unordered_map
// preserves the order on libstdc++ but reverses it on libc++, and within a bucket on
// MSVC. So the names are appended only once the function is in the catalog, read
// through the same by-value copy, never from the function before registration.
template <class ENTRY>
void AppendNamedParameters(ExtensionLoader &loader, CatalogType type, const string &name) {
	auto &db = loader.GetDatabaseInstance();
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = Catalog::GetSystemCatalog(db).GetSchema(transaction, DEFAULT_SCHEMA);
	auto entry = schema.GetEntry(transaction, type, name);
	if (!entry) {
		throw InternalException("RegisterDocumented: '%s' is not in the catalog after registration", name);
	}
	auto &function_entry = entry->Cast<ENTRY>();
	// Every function here registers exactly one overload, so one copy serves all.
	auto function = function_entry.functions.GetFunctionByOffset(0);
	for (auto &description : function_entry.descriptions) {
		for (auto &param : function.named_parameters) {
			description.parameter_names.push_back(param.first);
		}
	}
}

} // namespace function_docs_detail

// The bare loader.RegisterFunction(fn) overloads have nowhere to put a
// description. These register the same function with one, and with the same
// conflict behaviour the bare overloads have.
inline void RegisterDocumented(ExtensionLoader &loader, ScalarFunction function, const FunctionDoc &doc) {
	CreateScalarFunctionInfo info(std::move(function));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

inline void RegisterDocumented(ExtensionLoader &loader, TableFunction function, const FunctionDoc &doc) {
	auto name = function.name;
	CreateTableFunctionInfo info(std::move(function));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
	function_docs_detail::AppendNamedParameters<TableFunctionCatalogEntry>(loader, CatalogType::TABLE_FUNCTION_ENTRY,
	                                                                       name);
}

// ExtensionLoader has no overload taking a CreatePragmaFunctionInfo, so this does
// what its RegisterFunction(PragmaFunctionSet) does, with the description attached.
// Like that overload, it keeps the default ERROR_ON_CONFLICT.
inline void RegisterDocumented(ExtensionLoader &loader, PragmaFunction function, const FunctionDoc &doc) {
	auto name = function.name;
	PragmaFunctionSet set(name);
	set.AddFunction(std::move(function));
	CreatePragmaFunctionInfo info(name, std::move(set));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc));
	auto &db = loader.GetDatabaseInstance();
	Catalog::GetSystemCatalog(db).CreatePragmaFunction(CatalogTransaction::GetSystemTransaction(db), info);
	function_docs_detail::AppendNamedParameters<PragmaFunctionCatalogEntry>(loader, CatalogType::PRAGMA_FUNCTION_ENTRY,
	                                                                        name);
}

} // namespace cityjson
} // namespace duckdb

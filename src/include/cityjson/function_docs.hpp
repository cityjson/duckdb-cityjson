#pragma once

#include "duckdb.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
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
	// Positional parameters only. Named parameters are appended from the function
	// itself; see NamedParameterOrder.
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

// duckdb_functions() lists a table or pragma function's named parameters after its
// positional ones, in the iteration order of its named_parameters map, and a
// description's parameter_names replace that whole list. So the names must come
// from the same map, never from a hand-written list: its order is hash-dependent.
template <class FUNCTION>
vector<string> NamedParameterOrder(const FUNCTION &function) {
	vector<string> names;
	for (auto &param : function.named_parameters) {
		names.push_back(param.first);
	}
	return names;
}

inline FunctionDescription ToDescription(const FunctionDoc &doc, vector<string> named) {
	FunctionDescription description;
	// parameter_types stays empty: an empty list matches every overload, ANY included.
	description.parameter_names = doc.parameters;
	for (auto &name : named) {
		description.parameter_names.push_back(std::move(name));
	}
	description.description = doc.description;
	description.examples = {doc.example};
	description.categories = doc.categories;
	return description;
}

} // namespace function_docs_detail

// The bare loader.RegisterFunction(fn) overloads have nowhere to put a
// description. These register the same function with one, and with the same
// conflict behaviour the bare overloads have.
inline void RegisterDocumented(ExtensionLoader &loader, ScalarFunction function, const FunctionDoc &doc) {
	CreateScalarFunctionInfo info(std::move(function));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc, {}));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

inline void RegisterDocumented(ExtensionLoader &loader, TableFunction function, const FunctionDoc &doc) {
	auto named = function_docs_detail::NamedParameterOrder(function);
	CreateTableFunctionInfo info(std::move(function));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc, std::move(named)));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

// ExtensionLoader has no overload taking a CreatePragmaFunctionInfo, so this does
// what its RegisterFunction(PragmaFunctionSet) does, with the description attached.
// Like that overload, it keeps the default ERROR_ON_CONFLICT.
inline void RegisterDocumented(ExtensionLoader &loader, PragmaFunction function, const FunctionDoc &doc) {
	auto named = function_docs_detail::NamedParameterOrder(function);
	auto name = function.name;
	PragmaFunctionSet set(name);
	set.AddFunction(std::move(function));
	CreatePragmaFunctionInfo info(std::move(name), std::move(set));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc, std::move(named)));
	auto &db = loader.GetDatabaseInstance();
	Catalog::GetSystemCatalog(db).CreatePragmaFunction(CatalogTransaction::GetSystemTransaction(db), info);
}

} // namespace cityjson
} // namespace duckdb

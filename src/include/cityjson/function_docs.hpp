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
	// The positional parameters' names. duckdb_functions() reads every name from
	// the signature: these, then the named parameters a function declares as its
	// "options" kwargs.
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

// Gives the positional parameters their documented names. A STANDARD parameter
// takes its argument by position or by name; the type-only constructors declare
// positional-only parameters named col<N>, whose names a caller cannot use.
inline void NameParameters(FunctionSignature &signature, const vector<string> &names) {
	for (idx_t i = 0; i < signature.GetParameterCount() && i < names.size(); i++) {
		auto &parameter = signature.GetParameter(i);
		if (!parameter.AcceptsPosition()) {
			break;
		}
		optional<Value> default_value;
		if (parameter.HasDefaultValue()) {
			default_value = *parameter.GetDefaultValue();
		}
		parameter = FunctionParameter(Identifier(names[i]), parameter.GetType(), std::move(default_value),
		                              FunctionParameterKind::STANDARD);
	}
}

} // namespace function_docs_detail

// The bare loader.RegisterFunction(fn) overloads have nowhere to put a
// description. These register the same function with one, and with the same
// conflict behaviour the bare overloads have. A scalar is registered fallible:
// every one here parses or validates its input, and DuckDB turns an error from
// a function not marked fallible into an internal error.
inline void RegisterDocumented(ExtensionLoader &loader, ScalarFunction function, const FunctionDoc &doc) {
	function_docs_detail::NameParameters(function.GetSignature(), doc.parameters);
	function.SetFallible();
	CreateScalarFunctionInfo info(std::move(function));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

inline void RegisterDocumented(ExtensionLoader &loader, TableFunction function, const FunctionDoc &doc) {
	function_docs_detail::NameParameters(function.GetSignature(), doc.parameters);
	CreateTableFunctionInfo info(std::move(function));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	loader.RegisterFunction(std::move(info));
}

// ExtensionLoader has no overload taking a CreatePragmaFunctionInfo, so this does
// what its RegisterFunction(PragmaFunctionSet) does, with the description attached.
// Like that overload, it keeps the default ERROR_ON_CONFLICT.
inline void RegisterDocumented(ExtensionLoader &loader, PragmaFunction function, const FunctionDoc &doc) {
	function_docs_detail::NameParameters(function.GetSignature(), doc.parameters);
	auto name = function.GetName();
	PragmaFunctionSet set(name);
	set.AddFunction(std::move(function));
	CreatePragmaFunctionInfo info(name, std::move(set));
	info.descriptions.push_back(function_docs_detail::ToDescription(doc));
	auto &db = loader.GetDatabaseInstance();
	Catalog::GetSystemCatalog(db).CreatePragmaFunction(CatalogTransaction::GetSystemTransaction(db), info);
}

} // namespace cityjson
} // namespace duckdb

#include "cityjson/copy_source_ref.hpp"

#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/query_node/set_operation_node.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

#include <vector>

namespace duckdb {
namespace cityjson {

namespace {

void Collect(const QueryNode &node, std::vector<CopySourceRef> &out);

void CollectFromTableRef(const TableRef &ref, std::vector<CopySourceRef> &out) {
	switch (ref.type) {
	case TableReferenceType::TABLE_FUNCTION: {
		auto &fn = ref.Cast<TableFunctionRef>();
		if (!fn.function || fn.function->GetExpressionType() != ExpressionType::FUNCTION) {
			return;
		}
		auto &call = fn.function->Cast<FunctionExpression>();

		CopySourceRef found;
		if (call.function_name == "read_cityjson") {
			found.is_seq = false;
		} else if (call.function_name == "read_cityjsonseq") {
			found.is_seq = true;
		} else if (call.function_name == "read_flatcitybuf") {
			found.is_fcb = true;
		} else {
			return;
		}

		// Only a literal path is recoverable. A computed or parameterised argument
		// is not knowable at bind time, and a wrong guess is worse than none.
		if (call.children.empty() || call.children[0]->GetExpressionType() != ExpressionType::VALUE_CONSTANT) {
			return;
		}
		auto &constant = call.children[0]->Cast<ConstantExpression>();
		if (constant.value.IsNull() || constant.value.type().id() != LogicalTypeId::VARCHAR) {
			return;
		}
		found.path = StringValue::Get(constant.value);
		out.push_back(std::move(found));
		return;
	}
	case TableReferenceType::JOIN: {
		auto &join = ref.Cast<JoinRef>();
		if (join.left) {
			CollectFromTableRef(*join.left, out);
		}
		if (join.right) {
			CollectFromTableRef(*join.right, out);
		}
		return;
	}
	case TableReferenceType::SUBQUERY: {
		auto &sub = ref.Cast<SubqueryRef>();
		if (sub.subquery && sub.subquery->node) {
			Collect(*sub.subquery->node, out);
		}
		return;
	}
	default:
		// BASE_TABLE and everything else names no file. `COPY my_table TO ...` lands
		// here, which is why the explicit metadata_from option exists.
		return;
	}
}

void Collect(const QueryNode &node, std::vector<CopySourceRef> &out) {
	switch (node.type) {
	case QueryNodeType::SELECT_NODE: {
		auto &select = node.Cast<SelectNode>();
		if (select.from_table) {
			CollectFromTableRef(*select.from_table, out);
		}
		return;
	}
	case QueryNodeType::SET_OPERATION_NODE: {
		auto &setop = node.Cast<SetOperationNode>();
		for (auto &child : setop.children) {
			if (child) {
				Collect(*child, out);
			}
		}
		return;
	}
	default:
		return;
	}
}

} // namespace

std::optional<CopySourceRef> FindCopySourceRef(const QueryNode &node) {
	std::vector<CopySourceRef> found;
	Collect(node, found);
	if (found.size() != 1) {
		// Zero: nothing to inherit from. More than one: which source's CRS would we
		// claim? Both answers are "do not guess".
		return std::nullopt;
	}
	return found[0];
}

} // namespace cityjson
} // namespace duckdb

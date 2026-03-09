// seqscan_as_indexscan.cpp

#include "optimizer/optimizer.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

auto Optimizer::OptimizeSeqScanAsIndexScan(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }
  const auto &seq_scan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

  if (seq_scan.filter_predicate_ == nullptr) {
    return optimized_plan;
  }

  // 检查是否是 ComparisonExpression
  const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan.filter_predicate_.get());
  if (comp_expr == nullptr || comp_expr->comp_type_ != ComparisonType::Equal) {
    return optimized_plan;
  }

  auto left = comp_expr->GetChildAt(0);
  auto right = comp_expr->GetChildAt(1);

  const ColumnValueExpression *col_expr = nullptr;
  const ConstantValueExpression *const_expr = nullptr;

  // col = constant
  if (dynamic_cast<const ColumnValueExpression *>(left.get()) != nullptr &&
      dynamic_cast<const ConstantValueExpression *>(right.get()) != nullptr) {
    col_expr = dynamic_cast<const ColumnValueExpression *>(left.get());
    const_expr = dynamic_cast<const ConstantValueExpression *>(right.get());
  }
  // constant = col
  else if (dynamic_cast<const ConstantValueExpression *>(left.get()) != nullptr &&
           dynamic_cast<const ColumnValueExpression *>(right.get()) != nullptr) {
    const_expr = dynamic_cast<const ConstantValueExpression *>(left.get());
    col_expr = dynamic_cast<const ColumnValueExpression *>(right.get());
  } else {
    return optimized_plan;
  }

  auto col_idx = col_expr->GetColIdx();
  auto indexes = catalog_.GetTableIndexes(seq_scan.table_name_);
  
  for (const auto &index_info : indexes) {
    if (index_info->key_schema_.GetColumnCount() != 1) {
      continue;
    }

    auto index_col_name = index_info->key_schema_.GetColumn(0).GetName();
    auto table_col_name = catalog_.GetTable(seq_scan.table_oid_)->schema_.GetColumn(col_idx).GetName();

    if (index_col_name == table_col_name) {
      return std::make_shared<IndexScanPlanNode>(
          seq_scan.output_schema_,
          seq_scan.table_oid_,
          index_info->index_oid_,
          nullptr,
          const_expr
      );
    }
  }

  return optimized_plan;
}

}  // namespace bustub



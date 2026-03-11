#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
#include "execution/expressions/logic_expression.h"

namespace bustub {

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() != PlanType::NestedLoopJoin) {
    return optimized_plan;
  }
  const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
  
  if (nlj_plan.Predicate() == nullptr) {
    return optimized_plan;
  }
  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;
  std::vector<AbstractExpressionRef> exprs{nlj_plan.Predicate()};
  
  while (!exprs.empty()) {
    auto expr = exprs.back();
    exprs.pop_back();
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get())) {
      if (logic_expr->logic_type_ == LogicType::And) {
        exprs.push_back(logic_expr->children_[1]);
        exprs.push_back(logic_expr->children_[0]);
        continue;
      }
      return optimized_plan;
    }
    const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
    if (cmp_expr == nullptr || cmp_expr->comp_type_ != ComparisonType::Equal) {
      return optimized_plan;
    }
    auto *left_col = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[0].get());
    auto *right_col = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[1].get());
    if (left_col == nullptr || right_col == nullptr) {
      return optimized_plan;
    }
    if (left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1) {
      left_keys.emplace_back(cmp_expr->children_[0]);
      right_keys.emplace_back(cmp_expr->children_[1]);
    } else if (left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0) {
      left_keys.emplace_back(cmp_expr->children_[1]);
      right_keys.emplace_back(cmp_expr->children_[0]);
    } else {
      return optimized_plan;
    }
  }
  return std::make_shared<HashJoinPlanNode>(
      nlj_plan.output_schema_,
      nlj_plan.GetLeftPlan(),
      nlj_plan.GetRightPlan(),
      std::move(left_keys),
      std::move(right_keys),
      nlj_plan.GetJoinType()
  );
}
}  // namespace bustub

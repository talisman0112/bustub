#include "execution/plans/abstract_plan.h"
#include "optimizer/optimizer.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"

namespace bustub {

static auto IsJoinKey(const AbstractExpressionRef &expr, size_t left_col_cnt, size_t right_col_cnt)
    -> std::pair<bool, bool> {
  const auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (cmp_expr == nullptr || cmp_expr->comp_type_ != ComparisonType::Equal) {
    return {false, false};
  }
  auto *left_col = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[0].get());
  auto *right_col = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[1].get());
  if (left_col == nullptr || right_col == nullptr) {
    return {false, false};
  }
  bool left_from_left = left_col->GetTupleIdx() == 0 && left_col->GetColIdx() < left_col_cnt;
  bool left_from_right = left_col->GetTupleIdx() == 1 && left_col->GetColIdx() < right_col_cnt;
  bool right_from_left = right_col->GetTupleIdx() == 0 && right_col->GetColIdx() < left_col_cnt;
  bool right_from_right = right_col->GetTupleIdx() == 1 && right_col->GetColIdx() < right_col_cnt;
  if ((left_from_left && right_from_right) || (left_from_right && right_from_left)) {
    return {true, left_from_left && right_from_right};
  }
  return {false, false};
}

static auto ExtractJoinKeys(const AbstractExpressionRef &expr, size_t left_col_cnt, size_t right_col_cnt)
    -> std::pair<std::vector<AbstractExpressionRef>, std::vector<AbstractExpressionRef>> {
  std::vector<AbstractExpressionRef> left_keys;
  std::vector<AbstractExpressionRef> right_keys;
  std::vector<AbstractExpressionRef> remaining;
  std::vector<AbstractExpressionRef> stack{expr};
  while (!stack.empty()) {
    auto e = stack.back();
    stack.pop_back();
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(e.get())) {
      if (logic_expr->logic_type_ == LogicType::And) {
        stack.push_back(logic_expr->children_[0]);
        stack.push_back(logic_expr->children_[1]);
        continue;
      }
      remaining.push_back(e);
      continue;
    }
    auto [is_join, left_first] = IsJoinKey(e, left_col_cnt, right_col_cnt);
    if (is_join) {
      const auto *cmp = dynamic_cast<const ComparisonExpression *>(e.get());
      if (left_first) {
        left_keys.push_back(cmp->children_[0]);
        right_keys.push_back(cmp->children_[1]);
      } else {
        left_keys.push_back(cmp->children_[1]);
        right_keys.push_back(cmp->children_[0]);
      }
    } else {
      remaining.push_back(e);
    }
  }
  return {std::move(left_keys), std::move(right_keys)};
}

static auto CombineExpressions(const std::vector<AbstractExpressionRef> &exprs) -> AbstractExpressionRef {
  if (exprs.empty()) {
    return nullptr;
  }
  if (exprs.size() == 1) {
    return exprs[0];
  }
  auto result = exprs[0];
  for (size_t i = 1; i < exprs.size(); ++i) {
    result = std::make_shared<LogicExpression>(result, exprs[i], LogicType::And);
  }
  return result;
}

auto Optimizer::OptimizeCustom(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  auto p = plan;
  p = OptimizeMergeProjection(p);
  p = OptimizeMergeFilterNLJ(p);
  p = OptimizeNLJAsHashJoin(p);
  p = OptimizePushDownFilterToHashJoin(p);
  p = OptimizeOrderByAsIndexScan(p);
  p = OptimizeSortLimitAsTopN(p);
  p = OptimizeMergeFilterScan(p);
  p = OptimizeSeqScanAsIndexScan(p);
  return p;
}

auto Optimizer::OptimizePushDownFilterToHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizePushDownFilterToHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() != PlanType::Filter) {
    return optimized_plan;
  }
  const auto &filter_plan = dynamic_cast<const FilterPlanNode &>(*optimized_plan);
  const auto &child_plan = optimized_plan->children_[0];
  if (child_plan->GetType() != PlanType::HashJoin) {
    return optimized_plan;
  }
  const auto &hash_join = dynamic_cast<const HashJoinPlanNode &>(*child_plan);
  auto left_col_cnt = hash_join.GetLeftPlan()->OutputSchema().GetColumnCount();
  auto right_col_cnt = hash_join.GetRightPlan()->OutputSchema().GetColumnCount();
  auto predicate = filter_plan.GetPredicate();
  auto [left_join_keys, right_join_keys] = ExtractJoinKeys(predicate, left_col_cnt, right_col_cnt);
  if (left_join_keys.empty()) {
    return optimized_plan;
  }
  auto left_filter_exprs = std::vector<AbstractExpressionRef>{};
  auto right_filter_exprs = std::vector<AbstractExpressionRef>{};
  std::vector<AbstractExpressionRef> stack{predicate};
  while (!stack.empty()) {
    auto e = stack.back();
    stack.pop_back();
    if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(e.get())) {
      if (logic_expr->logic_type_ == LogicType::And) {
        stack.push_back(logic_expr->children_[0]);
        stack.push_back(logic_expr->children_[1]);
        continue;
      }
    }
    auto [is_join, left_first] = IsJoinKey(e, left_col_cnt, right_col_cnt);
    if (!is_join) {
      const auto *cmp = dynamic_cast<const ComparisonExpression *>(e.get());
      if (cmp != nullptr) {
        auto *col = dynamic_cast<const ColumnValueExpression *>(cmp->children_[0].get());
        if (col != nullptr) {
          if (col->GetTupleIdx() == 0) {
            left_filter_exprs.push_back(e);
          } else {
            right_filter_exprs.push_back(e);
          }
        } else {
          left_filter_exprs.push_back(e);
          right_filter_exprs.push_back(e);
        }
      } else {
        left_filter_exprs.push_back(e);
        right_filter_exprs.push_back(e);
      }
    }
  }
  auto left_filter = CombineExpressions(left_filter_exprs);
  auto right_filter = CombineExpressions(right_filter_exprs);
  AbstractPlanNodeRef new_left = hash_join.GetLeftPlan();
  if (left_filter != nullptr) {
    new_left = std::make_shared<FilterPlanNode>(hash_join.GetLeftPlan()->OutputSchema(), left_filter, new_left);
  }
  AbstractPlanNodeRef new_right = hash_join.GetRightPlan();
  if (right_filter != nullptr) {
    new_right = std::make_shared<FilterPlanNode>(hash_join.GetRightPlan()->OutputSchema(), right_filter, new_right);
  }
  std::vector<AbstractExpressionRef> all_left_keys = left_join_keys;
  std::vector<AbstractExpressionRef> all_right_keys = right_join_keys;
  for (size_t i = 0; i < hash_join.LeftJoinKeyExpressions().size(); ++i) {
    all_left_keys.push_back(hash_join.LeftJoinKeyExpressions()[i]);
    all_right_keys.push_back(hash_join.RightJoinKeyExpressions()[i]);
  }
  return std::make_shared<HashJoinPlanNode>(
      hash_join.output_schema_,
      new_left,
      new_right,
      std::move(all_left_keys),
      std::move(all_right_keys),
      hash_join.GetJoinType()
  );
}

}  // namespace bustub

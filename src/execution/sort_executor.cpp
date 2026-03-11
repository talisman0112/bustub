#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor))  {}

void SortExecutor::Init() {
  child_executor_->Init();
  sorted_tuples_.clear();Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    sorted_tuples_.push_back(tuple);
  }
  
  auto &order_bys = plan_->GetOrderBy();
  auto &schema = child_executor_->GetOutputSchema();
  
  std::sort(sorted_tuples_.begin(), sorted_tuples_.end(), 
    [&order_bys, &schema](const Tuple &a, const Tuple &b) {
      for (const auto &[order_type, expr] : order_bys) {
        Value val_a = expr->Evaluate(&a, schema);
        Value val_b = expr->Evaluate(&b, schema);
        
        if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
          continue;
        }
        
        bool less = val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
        return (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) ? less : !less;
      }
      return false;
    });
  iter_ = sorted_tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == sorted_tuples_.end()) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
}
} // namespace bustub

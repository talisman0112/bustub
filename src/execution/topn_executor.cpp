#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  top_entries_.clear();
  auto &order_bys = plan_->GetOrderBy();
  auto &schema = child_executor_->GetOutputSchema();
  std::size_t n = plan_->GetN();

  // 堆比较器：DESC 用小顶堆（a > b），ASC 用大顶堆（a < b）
  auto heap_cmp = [&order_bys, &schema](const Tuple &a, const Tuple &b) {
    for (const auto &[order_type, expr] : order_bys) {
      Value val_a = expr->Evaluate(&a, schema);
      Value val_b = expr->Evaluate(&b, schema);
      if (val_a.CompareEquals(val_b) == CmpBool::CmpTrue) {
        continue;
      }
      bool less = val_a.CompareLessThan(val_b) == CmpBool::CmpTrue;
      // DESC: 小顶堆 → a > b 时返回 true
      // ASC:  大顶堆 → a < b 时返回 true
      return (order_type == OrderByType::DESC) ? !less : less;
    }
    return false;
  };Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    top_entries_.push_back(tuple);
    std::push_heap(top_entries_.begin(), top_entries_.end(), heap_cmp);
    if (top_entries_.size() > n) {
      std::pop_heap(top_entries_.begin(), top_entries_.end(), heap_cmp);
      top_entries_.pop_back();
    }
  }

  // 最终排序：DESC → 大到小，ASC → 小到大
  std::sort(top_entries_.begin(), top_entries_.end(),
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
  iter_ = top_entries_.begin();
}



auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (iter_ == top_entries_.end()) {
    return false;
  }
  *tuple = *iter_;
  *rid = tuple->GetRid();
  ++iter_;
  return true;
 }

auto TopNExecutor::GetNumInHeap() -> size_t { return top_entries_.size(); };

}  // namespace bustub

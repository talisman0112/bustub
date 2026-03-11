#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
#include "type/value_factory.h"
#include <unordered_set>
#include <vector>
#include <numeric>
namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::Init() {
  child_executor_->Init();
  results_.clear();

  std::vector<Tuple> tuples;
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples.push_back(tuple);
  }

  const auto &child_schema = child_executor_->GetOutputSchema();
  const auto &columns = plan_->columns_;
  const auto &window_funcs = plan_->window_functions_;
  auto num_rows = static_cast<uint32_t>(tuples.size());
  auto num_cols = static_cast<uint32_t>(columns.size());

  results_.resize(num_rows, std::vector<Value>(num_cols));

  std::unordered_set<uint32_t> wf_cols;
  for (const auto &pair : window_funcs) {
    wf_cols.insert(pair.first);
  }
  for (uint32_t col = 0; col < num_cols; col++) {
    if (wf_cols.count(col) == 0) {
      for (uint32_t row = 0; row < num_rows; row++) {
        results_[row][col] = columns[col]->Evaluate(&tuples[row], child_schema);
      }
    }
  }

  for (const auto &pair : window_funcs) {
    uint32_t col_idx = pair.first;
    const auto &wf = pair.second;

    std::vector<uint32_t> order(num_rows);
    std::iota(order.begin(), order.end(), 0);

    const auto &partition_by = wf.partition_by_;
    const auto &order_by = wf.order_by_;
    const auto &func_type = wf.type_;
    const auto &func_expr = wf.function_;

    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) {
      for (const auto &expr : partition_by) {
        Value va = expr->Evaluate(&tuples[a], child_schema);
        Value vb = expr->Evaluate(&tuples[b], child_schema);
        if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
          return va.CompareLessThan(vb) == CmpBool::CmpTrue;
        }
      }
      for (const auto &ob : order_by) {
        Value va = ob.second->Evaluate(&tuples[a], child_schema);
        Value vb = ob.second->Evaluate(&tuples[b], child_schema);
        if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
          bool less = va.CompareLessThan(vb) == CmpBool::CmpTrue;
          return (ob.first == OrderByType::DESC) ? !less : less;
        }
      }
      return false;
    });

    bool has_order_by = !order_by.empty();

    auto same_partition = [&](uint32_t a, uint32_t b) -> bool {
      for (const auto &expr : partition_by) {
        Value va = expr->Evaluate(&tuples[a], child_schema);
        Value vb = expr->Evaluate(&tuples[b], child_schema);
        if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
          return false;
        }
      }
      return true;
    };

    auto same_order_key = [&](uint32_t a, uint32_t b) -> bool {
      for (const auto &ob : order_by) {
        Value va = ob.second->Evaluate(&tuples[a], child_schema);
        Value vb = ob.second->Evaluate(&tuples[b], child_schema);
        if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
          return false;
        }
      }
      return true;
    };

    auto init_agg = [&]() -> Value {
      switch (func_type) {
        case WindowFunctionType::CountStarAggregate:
          return ValueFactory::GetIntegerValue(0);
        default:
          return ValueFactory::GetNullValueByType(TypeId::INTEGER);
      }
    };

    auto combine = [&](const Value &old_val, const Value &new_val) -> Value {
      switch (func_type) {
        case WindowFunctionType::CountStarAggregate:
          return old_val.Add(ValueFactory::GetIntegerValue(1));
        case WindowFunctionType::CountAggregate:
          if (new_val.IsNull()) return old_val;
          if (old_val.IsNull()) return ValueFactory::GetIntegerValue(1);
          return old_val.Add(ValueFactory::GetIntegerValue(1));
        case WindowFunctionType::SumAggregate:
          if (new_val.IsNull()) return old_val;
          if (old_val.IsNull()) return new_val;
          return old_val.Add(new_val);
        case WindowFunctionType::MinAggregate:
          if (new_val.IsNull()) return old_val;
          if (old_val.IsNull()) return new_val;
          return old_val.Min(new_val);
        case WindowFunctionType::MaxAggregate:
          if (new_val.IsNull()) return old_val;
          if (old_val.IsNull()) return new_val;
          return old_val.Max(new_val);
        case WindowFunctionType::Rank:
          return old_val;
      }
      return old_val;
    };

    uint32_t i = 0;
    while (i < num_rows) {
      uint32_t part_start = i;
      uint32_t part_end = i + 1;
      while (part_end < num_rows && same_partition(order[part_start], order[part_end])) {
        part_end++;
      }

      if (!has_order_by) {
        if (func_type == WindowFunctionType::Rank) {
          for (uint32_t j = part_start; j < part_end; j++) {
            results_[order[j]][col_idx] = ValueFactory::GetIntegerValue(1);
          }
        } else {
          Value agg = init_agg();
          for (uint32_t j = part_start; j < part_end; j++) {
            Value val = func_expr->Evaluate(&tuples[order[j]], child_schema);
            agg = combine(agg, val);
          }
          for (uint32_t j = part_start; j < part_end; j++) {
            results_[order[j]][col_idx] = agg;
          }
        }
      } else {
        if (func_type == WindowFunctionType::Rank) {
          uint32_t rank = 1;
          for (uint32_t j = part_start; j < part_end;) {
            uint32_t peer_start = j;
            uint32_t peer_end = j + 1;
            while (peer_end < part_end && same_order_key(order[peer_start], order[peer_end])) {
              peer_end++;
            }
            for (uint32_t k = peer_start; k < peer_end; k++) {
              results_[order[k]][col_idx] = ValueFactory::GetIntegerValue(rank);
            }
            rank += (peer_end - peer_start);
            j = peer_end;
          }
        } else {
          Value agg = init_agg();
          for (uint32_t j = part_start; j < part_end;) {
            uint32_t peer_start = j;
            uint32_t peer_end = j + 1;
            while (peer_end < part_end && same_order_key(order[peer_start], order[peer_end])) {
              peer_end++;
            }
            for (uint32_t k = peer_start; k < peer_end; k++) {
              Value val = func_expr->Evaluate(&tuples[order[k]], child_schema);
              agg = combine(agg, val);
            }
            for (uint32_t k = peer_start; k < peer_end; k++) {
              results_[order[k]][col_idx] = agg;
            }
            j = peer_end;
          }
        }
      }

      i = part_end;
    }
  }

  // 重新排列 results_ 为排序后的顺序
  std::vector<uint32_t> final_order(num_rows);
  std::iota(final_order.begin(), final_order.end(), 0);

  if (!window_funcs.empty()) {
    const auto &wf = window_funcs.begin()->second;
    const auto &partition_by = wf.partition_by_;
    const auto &order_by = wf.order_by_;
    
    std::sort(final_order.begin(), final_order.end(), [&](uint32_t a, uint32_t b) {
      for (const auto &expr : partition_by) {
        Value va = expr->Evaluate(&tuples[a], child_schema);
        Value vb = expr->Evaluate(&tuples[b], child_schema);
        if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
          return va.CompareLessThan(vb) == CmpBool::CmpTrue;
        }
      }
      for (const auto &ob : order_by) {
        Value va = ob.second->Evaluate(&tuples[a], child_schema);
        Value vb = ob.second->Evaluate(&tuples[b], child_schema);
        if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
          bool less = va.CompareLessThan(vb) == CmpBool::CmpTrue;
          return (ob.first == OrderByType::DESC) ? !less : less;
        }
      }
      return false;
    });
    
    std::vector<std::vector<Value>> sorted_results(num_rows);
    for (uint32_t i = 0; i < num_rows; i++) {
      sorted_results[i] = std::move(results_[final_order[i]]);
    }
    results_ = std::move(sorted_results);
  }

  result_iter_ = results_.begin();
}


auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (result_iter_ == results_.end()) {
    return false;
  }
  *tuple = Tuple(*result_iter_, &GetOutputSchema());
  *rid = tuple->GetRid();
  ++result_iter_;
  return true;
}
}  // namespace bustub

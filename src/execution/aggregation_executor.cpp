//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)),aht_iterator_(aht_.End()) {}

void AggregationExecutor::Init() {
    child_executor_->Init();
    aht_.Clear();
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)){
    AggregateKey key = MakeAggregateKey(&tuple);
    AggregateValue value = MakeAggregateValue(&tuple);
    aht_.InsertCombine(key, value);
    }
    if (aht_.Begin() == aht_.End() && plan_->GetGroupBys().empty()) {
    aht_.InsertCombine({}, aht_.GenerateInitialAggregateValue());
    }
    aht_iterator_ = aht_.Begin();
  }
 
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (aht_iterator_ == aht_.End()) {
        return false;
    }
    const auto &key = aht_iterator_.Key();
    const auto &value = aht_iterator_.Val();
    std::vector<Value> values;
    for (const auto &group_by : key.group_bys_) {
        values.emplace_back(group_by);
    }
    for (const auto &agg : value.aggregates_) {
        values.emplace_back(agg);
    }
    *tuple = Tuple(values, &plan_->OutputSchema());
    ++aht_iterator_;
    return true;
 }

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub

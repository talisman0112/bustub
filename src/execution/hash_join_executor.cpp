//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "type/value_factory.h"


namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)){
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void HashJoinExecutor::Init() {
  right_executor_->Init();
  Tuple tuple;
  RID rid;
  while (right_executor_->Next(&tuple, &rid)) {
    std::vector<Value> key_values;
    // ✅ 遍历所有 join key
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      key_values.push_back(expr->Evaluate(&tuple, right_executor_->GetOutputSchema()));
    }
    
    // ✅ 用 AggregateKey 包装多个 Value
    CompositeKey key{key_values};
    hash_table_[key].push_back(tuple);
  }
  
  left_executor_->Init();
  current_matches_.clear();
}

auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!current_matches_.empty()) {
      Tuple right_tuple = current_matches_.back();  // ✅ 拷贝，不用引用
      current_matches_.pop_back();
      
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(right_tuple.GetValue(&right_executor_->GetOutputSchema(), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }

    if (!left_executor_->Next(&left_tuple_, rid)) {
      return false;
    }

    std::vector<Value> key_values;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      key_values.push_back(expr->Evaluate(&left_tuple_, left_executor_->GetOutputSchema()));
    }
    CompositeKey key{key_values};
    
    auto it = hash_table_.find(key);
    if (it != hash_table_.end() && !it->second.empty()) {
      current_matches_ = it->second;  // ✅ 拷贝整个 vector
    } else if (plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); i++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
  }
}


}  // namespace bustub

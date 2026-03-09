//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}

void UpdateExecutor::Init() { 
  child_executor_->Init();
  is_updated_ = false;
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
}

auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if (is_updated_)
    {
        return false;
    }
  auto *catalog = exec_ctx_->GetCatalog();
  auto *table_heap = table_info_->table_.get();
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
    int32_t count = 0;
    Tuple old_tuple;
    RID old_rid;
  while (child_executor_->Next(&old_tuple, &old_rid)) {
  
    // 2. 标记旧 tuple 删除
    table_heap->UpdateTupleMeta(TupleMeta{INVALID_TS, true}, old_rid);
    
    // 3. 生成新 tuple（用 target_expressions_ 计算新值）
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
        new_values.push_back(expr->Evaluate(&old_tuple, table_info_->schema_));
    }
    Tuple new_tuple(new_values, &table_info_->schema_);
    
    // 4. 插入新 tuple
    auto new_rid = table_heap->InsertTuple(TupleMeta{INVALID_TXN_ID, false}, new_tuple);
    
    // 5. 更新索引：删旧 key，插新 key
    for (auto *index_info : indexes) {
        auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                               index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_key, old_rid, exec_ctx_->GetTransaction());
        
        auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                               index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(new_key, new_rid.value(), exec_ctx_->GetTransaction());
    }
    
    count++;
}
    std::vector<Value> values{Value(TypeId::INTEGER, count)};
    *tuple = Tuple(values, &GetOutputSchema());
    is_updated_ = true;
    return true;
}
 }  // namespace bustub

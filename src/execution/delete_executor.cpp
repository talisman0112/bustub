//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    :AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}


void DeleteExecutor::Init() { 
    child_executor_->Init();
    is_done_=false;
 }

auto DeleteExecutor::Next( Tuple *tuple, RID *rid) -> bool {
    if (is_done_)
    {
        return false;
    }
    auto *catalog = exec_ctx_->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  auto *table_heap = table_info->table_.get();
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  TupleMeta meta{INVALID_TXN_ID, true};
    int32_t count = 0;
    Tuple child_tuple;
    RID child_rid;
    while (child_executor_->Next(&child_tuple, &child_rid)) {
    table_heap->UpdateTupleMeta(meta, child_rid);
    // 更新所有索引
    for (auto *index_info : indexes) {
      auto key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, 
                                          index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key, child_rid, exec_ctx_->GetTransaction());
    }
    count++;
 }
 
    std::vector<Value> values{Value(TypeId::INTEGER, count)};
    *tuple = Tuple(values, &GetOutputSchema());
    is_done_ = true;
    return true;
}
   
}  // namespace bustub

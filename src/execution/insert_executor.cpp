//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    :AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
    child_executor_->Init();
    executed_ = false;
 }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if (executed_) {
    return false;
  }
  auto *catalog = exec_ctx_->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  auto *table_heap = table_info->table_.get();
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto *txn = exec_ctx_->GetTransaction();
  int32_t count = 0;
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    TupleMeta meta{txn->GetTransactionTempTs(), false};  
    auto insert_rid = table_heap->InsertTuple(meta, child_tuple);
    if (!insert_rid.has_value()) {
      continue;  
    }
    txn->AppendWriteSet(plan_->GetTableOid(), *insert_rid);
    for (auto *index_info : indexes) {
      auto key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, 
                                          index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, insert_rid.value(), exec_ctx_->GetTransaction());
    }
    count++;
  }
  std::vector<Value> values{{TypeId::INTEGER, count}};
  *tuple = Tuple{values, &GetOutputSchema()};
  executed_ = true;
  return true;
 }

}  // namespace bustub

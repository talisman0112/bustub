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

  // 找到 primary key index
  IndexInfo *pk_index = nullptr;
  for (auto *idx : indexes) {
    if (idx->is_primary_key_) {
      pk_index = idx;
      break;
    }
  }

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    
    // Step 1: 先检查 primary key 是否冲突（在插入 heap 之前）
    if (pk_index != nullptr) {
      auto key = child_tuple.KeyFromTuple(table_info->schema_, pk_index->key_schema_,
                                          pk_index->index_->GetKeyAttrs());

      std::vector<RID> result;
      pk_index->index_->ScanKey(key, &result, txn);

      for (const auto &existing_rid : result) {
        auto [existing_meta, existing_tuple] = table_heap->GetTuple(existing_rid);
        // 已删除的跳过
        if (existing_meta.is_deleted_ && existing_meta.ts_ == txn->GetTransactionTempTs()) {
        continue;
      }
    
      // 已提交的删除，且对我可见，可以重用
        if (existing_meta.is_deleted_ && existing_meta.ts_ <= txn->GetReadTs()) {
        continue;
        }

        // 未提交的其他事务 → write-write conflict
        if (existing_meta.ts_ > TXN_START_ID) {
          if (existing_meta.ts_ != txn->GetTransactionTempTs()) {
            txn->SetTainted();
            throw ExecutionException("write-write conflict on primary key");
          }
          // 自己的未提交数据，也是冲突（同一事务内重复插入相同 key）
          txn->SetTainted();
          throw ExecutionException("duplicate key in same transaction");
        }

        // 已提交且对我可见 → primary key conflict
        if (existing_meta.ts_ <= txn->GetReadTs()) {
          txn->SetTainted();
          throw ExecutionException("primary key conflict - key already exists");
        }

        // ts > read_ts 且 ts <= TXN_START_ID：在我开始后提交的，对我不可见，不冲突
      }
    }

    // Step 2: 检查通过，插入 table heap
    TupleMeta meta{txn->GetTransactionTempTs(), false};
    auto insert_rid_opt = table_heap->InsertTuple(meta, child_tuple);
    if (!insert_rid_opt.has_value()) {
      continue;
    }
    RID insert_rid = *insert_rid_opt;

    // Step 3: 插入 primary key index
    if (pk_index != nullptr) {
      auto key = child_tuple.KeyFromTuple(table_info->schema_, pk_index->key_schema_,
                                          pk_index->index_->GetKeyAttrs());

      bool success = pk_index->index_->InsertEntry(key, insert_rid, txn);

      if (!success) {
        // 并发冲突：在 step1 检查和 step3 插入之间，其他 txn 抢先插入了
        // 回滚 heap 插入
        table_heap->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, insert_rid);
        txn->SetTainted();
        throw ExecutionException("concurrent insert conflict on primary key");
      }
    }

    // Step 4: 插入 secondary indexes
    for (auto *index_info : indexes) {
      if (index_info->is_primary_key_) {
        continue;
      }
      auto key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                          index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, insert_rid, txn);
    }

    // Step 5: 记录 write set
    txn->AppendWriteSet(plan_->GetTableOid(), insert_rid);
    count++;
  }

  // 返回插入行数
  std::vector<Value> values{{TypeId::INTEGER, count}};
  *tuple = Tuple{values, &GetOutputSchema()};
  executed_ = true;
  return true;
}
}

 // namespace bustub

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
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    :AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}


void DeleteExecutor::Init() { 
    child_executor_->Init();
    is_done_=false;
 }

auto DeleteExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_done_) {
    return false;
  }

  auto *catalog = exec_ctx_->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  auto *table_heap = table_info->table_.get();
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_oid = plan_->GetTableOid();

  int32_t count = 0;
  Tuple child_tuple;
  RID child_rid;

 while (child_executor_->Next(&child_tuple, &child_rid)) {
  auto old_meta = table_heap->GetTupleMeta(child_rid);
  auto old_tuple = table_heap->GetTuple(child_rid).second;

  // Case 1: 自己本事务写的
  if (old_meta.ts_ == txn->GetTransactionTempTs()) {
    TupleMeta new_meta{txn->GetTransactionTempTs(), true};
    table_heap->UpdateTupleMeta(new_meta, child_rid);
  } 
  // Case 2: 别的未提交事务写的
  else if (old_meta.ts_ > TXN_START_ID) {
    txn->SetTainted();
    throw ExecutionException("Write-write conflict");
  }
  // Case 3: 已提交但 ts > read_ts（在我开始后被别人改过）
  else if (old_meta.ts_ > txn->GetReadTs()) {
    txn->SetTainted();
    throw ExecutionException("Write-write conflict");
  }
  // Case 4: 正常删除
  else {
    auto version_link = txn_mgr->GetVersionLink(child_rid);
    UndoLink prev_undo_link;
    if (version_link.has_value()) {
      prev_undo_link = version_link->prev_;
    }

    std::vector<bool> modified_fields(table_info->schema_.GetColumnCount(), true);

    UndoLog undo_log;
    undo_log.is_deleted_ = old_meta.is_deleted_;
    undo_log.ts_ = old_meta.ts_;
    undo_log.modified_fields_ = modified_fields;
    undo_log.tuple_ = old_tuple;
    undo_log.prev_version_ = prev_undo_link;

    auto new_undo_link = txn->AppendUndoLog(undo_log);

    VersionUndoLink new_version_link;
    new_version_link.prev_ = new_undo_link;
    new_version_link.in_progress_ = true;
    txn_mgr->UpdateVersionLink(child_rid, new_version_link);

    TupleMeta new_meta{txn->GetTransactionTempTs(), true};
    table_heap->UpdateTupleMeta(new_meta, child_rid);
    txn->AppendWriteSet(table_oid, child_rid);
  }

  // 删除索引
  for (auto *index_info : indexes) {
    auto key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                        index_info->index_->GetKeyAttrs());
    index_info->index_->DeleteEntry(key, child_rid, txn);
  }

  count++;
}

  std::vector<Value> values{Value(TypeId::INTEGER, count)};
  *tuple = Tuple(values, &GetOutputSchema());
  is_done_ = true;
  return true;
}
   
}  // namespace bustub

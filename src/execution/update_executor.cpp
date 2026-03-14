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
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
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
  if (is_updated_) {
    return false;
  }

  auto *catalog = exec_ctx_->GetCatalog();
  auto *table_heap = table_info_->table_.get();
  auto indexes = catalog->GetTableIndexes(table_info_->name_);
  auto *txn = exec_ctx_->GetTransaction();
  auto *txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_oid = plan_->GetTableOid();

  int32_t count = 0;
  Tuple old_tuple;
  RID old_rid;

  while (child_executor_->Next(&old_tuple, &old_rid)) {
    // 1. 获取旧 meta
    auto old_meta = table_heap->GetTupleMeta(old_rid);

    // 2. 生成新 tuple
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&old_tuple, table_info_->schema_));
    }
    Tuple new_tuple(new_values, &table_info_->schema_);

    // 3. 获取当前 version link
    auto version_link = txn_mgr->GetVersionLink(old_rid);
    UndoLink prev_undo_link;
    if (version_link.has_value()) {
      prev_undo_link = version_link->prev_;
    }

    // 4. 检查是否是 self-modification
    if (old_meta.ts_ == txn->GetTransactionTempTs()) {
      // 自己刚插入/修改的，直接原地更新，不需要新 undo log
      table_heap->UpdateTupleInPlace(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple, old_rid);
    } else {
      // 5. 创建 Undo Log 保存旧版本
      std::vector<bool> modified_fields(table_info_->schema_.GetColumnCount(), true);

      UndoLog undo_log;
      undo_log.is_deleted_ = old_meta.is_deleted_;
      undo_log.ts_ = old_meta.ts_;
      undo_log.modified_fields_ = modified_fields;
      undo_log.tuple_ = old_tuple;
      undo_log.prev_version_ = prev_undo_link;

      // 6. 追加 undo log
      auto new_undo_link = txn->AppendUndoLog(undo_log);

      // 7. 更新 version link
      VersionUndoLink new_version_link;
      new_version_link.prev_ = new_undo_link;
      new_version_link.in_progress_ = true;
      txn_mgr->UpdateVersionLink(old_rid, new_version_link);

      // 8. 原地更新 tuple
      table_heap->UpdateTupleInPlace(TupleMeta{txn->GetTransactionTempTs(), false}, new_tuple, old_rid);

      // 9. 加入 write set
      txn->AppendWriteSet(table_oid, old_rid);
    }

    // 10. 更新索引
    for (auto *index_info : indexes) {
      auto old_key = old_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                            index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(old_key, old_rid, txn);

      auto new_key = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_,
                                            index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(new_key, old_rid, txn);  // 注意：RID 不变！
    }

    count++;
  }

  std::vector<Value> values{Value(TypeId::INTEGER, count)};
  *tuple = Tuple(values, &GetOutputSchema());
  is_updated_ = true;
  return true;
}
 }  // namespace bustub

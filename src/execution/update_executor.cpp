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
#include "execution/execution_common.h"
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
}
auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_updated_) {
    return false;
  }
  
  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto table_heap = table_info->table_.get();
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_oid = plan_->table_oid_;
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto column_count = table_info->schema_.GetColumnCount();
  
  Tuple child_tuple;
  RID child_rid;
  int32_t count = 0;
  
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    fmt::println(stderr, "UpdateExecutor: processing rid={}", child_rid.ToString());
    auto old_meta = table_heap->GetTupleMeta(child_rid);
    auto old_tuple = table_heap->GetTuple(child_rid).second;
    
    // 计算新 tuple
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&old_tuple, table_info->schema_));
    }
    Tuple new_tuple(new_values, &table_info->schema_);
    
    // 冲突检测 & undo log
    if (old_meta.ts_ == txn->GetTransactionTempTs()) {
      // Case 1: 自己之前写过 → 更新已有的 undo log
      auto version_link = txn_mgr->GetVersionLink(child_rid);
      
      if (version_link.has_value() && version_link->prev_.IsValid()) {
        auto undo_link = version_link->prev_;
        auto old_undo_log = txn_mgr->GetUndoLog(undo_link);
        
        // 重建原始 tuple
        auto reconstructed = ReconstructTuple(&table_info->schema_, old_tuple, old_meta, {old_undo_log});
        
        if (reconstructed.has_value()) {
          Tuple original_tuple = reconstructed.value();
          
          // 关键：合并 modified_fields（旧的 OR 新的）
          std::vector<bool> new_modified_fields = old_undo_log.modified_fields_;
          
          // 确保大小正确
          if (new_modified_fields.size() < column_count) {
            new_modified_fields.resize(column_count, false);
          }
          
          // 检查当前修改了哪些列，合并进去
          for (uint32_t i = 0; i < column_count; i++) {
            auto heap_value = old_tuple.GetValue(&table_info->schema_, i);
            auto new_value = new_tuple.GetValue(&table_info->schema_, i);
            if (!heap_value.CompareExactlyEquals(new_value)) {
              new_modified_fields[i] = true;
            }
          }
          
          // 根据合并后的 modified_fields 构建 undo values（用原始值）
          std::vector<Value> new_undo_values;
          std::vector<Column> new_undo_columns;
          
          for (uint32_t i = 0; i < column_count; i++) {
            if (new_modified_fields[i]) {
              new_undo_values.push_back(original_tuple.GetValue(&table_info->schema_, i));
              new_undo_columns.push_back(table_info->schema_.GetColumn(i));
            }
          }
          
          // 构建新的 undo log
          Schema new_partial_schema(new_undo_columns);
          Tuple new_partial_tuple(new_undo_values, &new_partial_schema);
          
          UndoLog new_undo_log;
          new_undo_log.is_deleted_ = old_undo_log.is_deleted_;
          new_undo_log.ts_ = old_undo_log.ts_;
          new_undo_log.modified_fields_ = new_modified_fields;
          new_undo_log.tuple_ = new_partial_tuple;
          new_undo_log.prev_version_ = old_undo_log.prev_version_;
          
          txn->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
        }
      }
      // 如果没有 valid undo link（自己 insert 的），不需要创建 undo log
      
    } else if (old_meta.ts_ > TXN_START_ID) {
      // Case 2: 别的未提交事务写的
      txn->SetTainted();
      throw ExecutionException("Write-write conflict: uncommitted txn");
      
    } else if (old_meta.ts_ > txn->GetReadTs()) {
      // Case 3: 已提交但 ts > read_ts
      txn->SetTainted();
      throw ExecutionException("Write-write conflict: committed after my read_ts");
      
    } else {
      // Case 4: 
      std::vector<bool> modified_fields(column_count, false);
      std::vector<Value> undo_values;
      std::vector<Column> undo_columns;
      
      for (uint32_t i = 0; i < column_count; i++) {
        auto old_value = old_tuple.GetValue(&table_info->schema_, i);
        auto new_value = new_tuple.GetValue(&table_info->schema_, i);
        if (!old_value.CompareExactlyEquals(new_value)) {
          modified_fields[i] = true;
          undo_values.push_back(old_value);
          undo_columns.push_back(table_info->schema_.GetColumn(i));
        }
      }
      
      // 获取之前的 version link
      auto version_link = txn_mgr->GetVersionLink(child_rid);
      UndoLink prev_undo_link;
      if (version_link.has_value()) {
        if(version_link->in_progress_){
        txn->SetTainted();
        throw ExecutionException("Write-write conflict: in_progress");
        }
        prev_undo_link = version_link->prev_; 
      }
      
      // 创建 undo log
      Schema partial_schema(undo_columns);
      Tuple partial_tuple(undo_values, &partial_schema);
      
      UndoLog undo_log;
      undo_log.is_deleted_ = old_meta.is_deleted_;
      undo_log.ts_ = old_meta.ts_;
      undo_log.modified_fields_ = modified_fields;
      undo_log.tuple_ = partial_tuple;
      undo_log.prev_version_ = prev_undo_link;
      
      auto new_undo_link = txn->AppendUndoLog(undo_log);
      
      // 更新 version link
      VersionUndoLink new_version_link;
    new_version_link.prev_ = new_undo_link;
    new_version_link.in_progress_ = true;
    bool success = txn_mgr->UpdateVersionLink(child_rid, new_version_link,
    [](std::optional<VersionUndoLink> old) {
        return !old.has_value() || !old->in_progress_;
    });
    if (!success) {
    txn->SetTainted();
    throw ExecutionException("Write-write conflict: in_progress");
    }
      
      txn->AppendWriteSet(table_oid, child_rid);
    }
    
    // 更新 table heap
    TupleMeta new_meta{txn->GetTransactionTempTs(), false};
    table_heap->UpdateTupleInPlace(new_meta, new_tuple, child_rid, nullptr);
    auto current_link = txn_mgr->GetVersionLink(child_rid);
    if (current_link.has_value()) {
    VersionUndoLink cleared_link = *current_link;
    cleared_link.in_progress_ = false;
    txn_mgr->UpdateVersionLink(child_rid, cleared_link, nullptr);  // 无条件更新
    }
    // 更新索引
    for (auto *index_info : indexes) {
    auto old_key = old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                          index_info->index_->GetKeyAttrs());
    auto new_key = new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                          index_info->index_->GetKeyAttrs());
    
    // 检查 key 是否变了
    bool key_changed = false;
    for (uint32_t i = 0; i < index_info->key_schema_.GetColumnCount(); i++) {
        if (!old_key.GetValue(&index_info->key_schema_, i)
                .CompareExactlyEquals(new_key.GetValue(&index_info->key_schema_, i))) {
            key_changed = true;
            break;
        }
    }
    
    if (key_changed) {
        // 只插入新 key，不删旧 key
        index_info->index_->InsertEntry(new_key, child_rid, txn);
    }
}
    
    count++;
  }
  fmt::println(stderr, "UpdateExecutor: total count={}", count);
  std::vector<Value> values{Value(TypeId::INTEGER, count)};
  *tuple = Tuple(values, &GetOutputSchema());
  is_updated_ = true;
  return true;
}
 }  // namespace bustub

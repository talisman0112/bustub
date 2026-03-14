//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() { 
    auto *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  table_iterator_.emplace(table_info_->table_->MakeIterator());
 }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto read_ts = txn->GetReadTs();
  auto txn_id = txn->GetTransactionTempTs();
  const auto *schema = &GetOutputSchema();

  while (!table_iterator_->IsEnd()) {
    RID cur_rid = table_iterator_->GetRID();
    auto [meta, base_tuple] = table_iterator_->GetTuple();

    // Case 1: 当前事务自己写的（uncommitted）
    if (meta.ts_ == txn_id) {
      ++(*table_iterator_);
      if (!meta.is_deleted_) {
        *tuple = base_tuple;
        *rid = cur_rid;
        return true;
      }
      // 自己删的，跳过
      continue;
    }

    // Case 2: 已提交且 ts <= read_ts，直接可见
    if (meta.ts_ <= read_ts) {
      ++(*table_iterator_);
      if (!meta.is_deleted_) {
        *tuple = base_tuple;
        *rid = cur_rid;
        return true;
      }
      // 已删除，跳过
      continue;
    }

    // Case 3: ts > read_ts，需要遍历 version chain
    std::vector<UndoLog> undo_logs;
    auto undo_link = txn_mgr->GetUndoLink(cur_rid);
    bool found_visible = false;

    while (undo_link.has_value() && undo_link->IsValid()) {
      auto undo_log = txn_mgr->GetUndoLog(*undo_link);
      undo_logs.push_back(undo_log);
      if (undo_log.ts_ <= read_ts) {
        found_visible = true;
        break;
      }
      undo_link = undo_log.prev_version_;
    }

    ++(*table_iterator_);

    if (found_visible) {
      auto reconstructed = ReconstructTuple(schema, base_tuple, meta, undo_logs);
      if (reconstructed.has_value()) {
        *tuple = *reconstructed;
        *rid = cur_rid;
        return true;
      }
    }
  }

  return false;   
}
}  // namespace bustub

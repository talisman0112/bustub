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
  auto readts = txn->GetReadTs();
  auto txnid = txn->GetTransactionId();
  const auto *schema = &GetOutputSchema();

  while (!table_iterator_->IsEnd()) {
    RID cur_rid = table_iterator_->GetRID();
    auto [meta, base_tuple] = table_iterator_->GetTuple();

    if (meta.ts_ == txnid || meta.ts_ <= readts) {
      if (!meta.is_deleted_) {
        *tuple = base_tuple;
        *rid = cur_rid;
        ++(*table_iterator_);
        return true;
      }
    } else {
      std::vector<UndoLog> undo_logs;
  auto undo_link = txn_mgr->GetUndoLink(cur_rid);
  bool found_visible = false;
  while (undo_link.has_value() && undo_link->IsValid()) {
    auto undo_log = txn_mgr->GetUndoLog(*undo_link);
    undo_logs.push_back(undo_log);
    if (undo_log.ts_ <= readts) {
      found_visible = true;
      break;
    }
    undo_link = undo_log.prev_version_;
  }
  if (found_visible) {
    auto reconstructed = ReconstructTuple(schema, base_tuple, meta, undo_logs);
    if (reconstructed.has_value()) {
      *tuple = *reconstructed;
      *rid = cur_rid;
      ++(*table_iterator_);
      return true;
    }
  }
    }
    ++(*table_iterator_);
  }
  return false;   
}

}  // namespace bustub

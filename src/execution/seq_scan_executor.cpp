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
  auto txn_temp_ts = txn->GetTransactionTempTs();
  const auto *schema = &GetOutputSchema();

  while (!table_iterator_->IsEnd()) {
    RID cur_rid = table_iterator_->GetRID();
    auto [meta, base_tuple] = table_iterator_->GetTuple();
    ++(*table_iterator_);

    std::optional<Tuple> result_tuple;

    // Case 1: 当前事务自己写的
    if (meta.ts_ == txn_temp_ts) {
      if (meta.is_deleted_) {
        continue;
      }
      result_tuple = base_tuple;
    }
    // Case 2: 已提交且 ts <= read_ts
    else if (meta.ts_ <= read_ts) {
      if (meta.is_deleted_) {
        continue;
      }
      result_tuple = base_tuple;
    }
    // Case 3: 需要走 version chain
    else {
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

      if (found_visible) {
        result_tuple = ReconstructTuple(schema, base_tuple, meta, undo_logs);
      }
    }

    // 如果找到了可见的 tuple，应用 filter
    if (result_tuple.has_value()) {
      //  关键：应用 filter predicate
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&(*result_tuple), *schema);
        if (value.IsNull() || !value.GetAs<bool>()) {
          continue;  // 不满足条件，跳过
        }
      }
      
      *tuple = *result_tuple;
      *rid = cur_rid;
      return true;
    }
  }

  return false;   
}
}  // namespace bustub

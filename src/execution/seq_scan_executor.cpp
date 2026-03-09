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

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() { 
    auto *catalog = exec_ctx_->GetCatalog();
  table_info_ = catalog->GetTable(plan_->GetTableOid());
  table_iterator_.emplace(table_info_->table_->MakeIterator());
 }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  while (!table_iterator_->IsEnd()) {
    auto [meta, fetched_tuple] = table_iterator_->GetTuple();
    auto current_rid = table_iterator_->GetRID();
    ++(*table_iterator_);
    if (meta.is_deleted_) {
      continue;
    }
    if (plan_->filter_predicate_ != nullptr) {
      auto value = plan_->filter_predicate_->Evaluate(&fetched_tuple, table_info_->schema_);
      if (!value.GetAs<bool>()) {
        continue;
      }
    }
    *tuple = fetched_tuple;
    *rid = current_rid;
    return true;
  }
  return false;   
}

}  // namespace bustub

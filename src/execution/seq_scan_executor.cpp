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

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) : AbstractExecutor(exec_ctx) {}

void SeqScanExecutor::Init() { 
    auto *catalog=exec_ctx_->GetCatalog();
    auto *tableinfo=catalog->GetTable(plan_->GetTableOid());
    auto *table_heap = tableinfo->table_.get();
    auto  table_iterator_ = table_heap->MakeIterator();
 }

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { return false; }

}  // namespace bustub

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
  RID cur_rid=table_iterator_->GetRID();
  auto [meta, base_tuple] = table_iterator_->GetTuple();
  auto txn=exec_ctx_->GetTransaction();
  auto readts=txn->GetReadTs();
  auto txnid=txn->GetTransactionId();
  bool is_directly_visible = false;
  if(meta.ts_==txnid){
    is_directly_visible=!meta.is_deleted_;
  }
  else if(meta.ts_<=readts){
    is_directly_visible=!meta.is_deleted_;
  }
  else{
    is_directly_visible=false;
  }
  if (meta.ts_ == txnid || meta.ts_ <= readts) {
        if (!meta.is_deleted_) {
            *tuple = base_tuple;
            *rid = cur_rid;
            ++(*table_iterator_);
            return true;
        }
      }
    else {
        
    }
  }
  return false;   
}

}  // namespace bustub

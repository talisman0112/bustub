//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() { 
    auto *catalog = exec_ctx_->GetCatalog();
    index_info_ = catalog->GetIndex(plan_->GetIndexOid());
    table_info_ = catalog->GetTable(plan_->table_oid_);
    if (plan_->pred_key_ != nullptr) { 
        Value key_value = plan_->pred_key_->Evaluate(nullptr, table_info_->schema_);
        Tuple key_tuple({key_value}, &index_info_->key_schema_);
        auto *hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
        hash_index->ScanKey(key_tuple, &rids_, exec_ctx_->GetTransaction());
    }
    iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    auto txn=exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    const auto *schema = &GetOutputSchema();
    while (iter_ != rids_.end()) {
        RID cur_rid = *iter_;
        ++iter_; 
        auto [meta, current_tuple] = table_info_->table_->GetTuple(cur_rid);
        bool is_visible=false;
        if(meta.ts_==txn->GetTransactionTempTs()||meta.ts_<=txn->GetReadTs()){
         is_visible=true;
        }
        if(is_visible){
            if(meta.is_deleted_){
            *tuple = std::move(current_tuple);
            *rid=cur_rid;
            return true;
            }
        }
        
        else{
            std::vector<UndoLog> undo_logs;
            auto undo_link = txn_mgr->GetUndoLink(cur_rid);
            bool found_visible = false;

       while (undo_link.has_value() && undo_link->IsValid()) {
        auto undo_log = txn_mgr->GetUndoLog(*undo_link);
        undo_logs.push_back(undo_log);
        if (undo_log.ts_ <= txn->GetReadTs()) {
          found_visible = true;
          break;
        }
        undo_link = undo_log.prev_version_;
      }
      std::optional<Tuple> result_tuple;
      if (found_visible) {    
        result_tuple = ReconstructTuple(schema, current_tuple, meta, undo_logs);
        if (result_tuple.has_value()) {
        *tuple = std::move(*result_tuple);
        *rid = cur_rid;
        return true;
    }
      }
        }
    }
    return false;
 }

}  // namespace bustub

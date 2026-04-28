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
#include <chrono>
#include <fstream>
#include <string>
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

// #region agent log
namespace {
inline void BustubDebugLog(const char *location, const char *run_id, const char *hypothesis_id, const std::string &message,
                           const std::string &data_json) {
  try {
    std::ofstream out("debug-0d0b08.log", std::ios::app);
    if (!out.is_open()) {
      return;
    }
    const auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
    out << "{\"sessionId\":\"0d0b08\",\"timestamp\":" << ts << ",\"location\":\"" << location << "\",\"runId\":\""
        << run_id << "\",\"hypothesisId\":\"" << hypothesis_id << "\",\"message\":\"" << message << "\",\"data\":"
        << (data_json.empty() ? "{}" : data_json) << "}\n";
  } catch (...) {
  }
}
}  // namespace
// #endregion agent log

void IndexScanExecutor::Init() { 
    auto *catalog = exec_ctx_->GetCatalog();
    index_info_ = catalog->GetIndex(plan_->GetIndexOid());
    table_info_ = catalog->GetTable(plan_->table_oid_);
    rids_.clear();
    seen_rids_.clear();
    if (plan_->pred_key_ != nullptr) { 
        Value key_value = plan_->pred_key_->Evaluate(nullptr, table_info_->schema_);
        Tuple key_tuple({key_value}, &index_info_->key_schema_);
        auto *hash_index = dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get());
        hash_index->ScanKey(key_tuple, &rids_, exec_ctx_->GetTransaction());
        // #region agent log
        BustubDebugLog("index_scan_executor.cpp:Init:scankey", "pre-fix", "H5",
                       "IndexScan Init ScanKey finished",
                       std::string("{\"rids_size\":") + std::to_string(rids_.size()) + ",\"pred_key\":\"" +
                           key_value.ToString() + "\"}");
        // #endregion agent log
    }
    iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    auto txn=exec_ctx_->GetTransaction();
    auto txn_mgr = exec_ctx_->GetTransactionManager();
    const auto *schema = &table_info_->schema_;
    while (iter_ != rids_.end()) {
        RID cur_rid = *iter_;
        ++iter_; 
        if (seen_rids_.count(cur_rid) > 0) {
            continue;  
        }
        seen_rids_.insert(cur_rid);
        auto [meta, current_tuple] = table_info_->table_->GetTuple(cur_rid);
        bool is_visible=false;
        if(meta.ts_==txn->GetTransactionTempTs()||meta.ts_<=txn->GetReadTs()){
         is_visible=true;
        }
        if(is_visible){
            if(!meta.is_deleted_){
            *tuple = std::move(current_tuple);
            *rid=cur_rid;
            // #region agent log
            try {
              auto k0 = tuple->GetValue(*schema, 0).ToString();
              std::string pred_key = "null";
              if (plan_->pred_key_ != nullptr) {
                pred_key = plan_->pred_key_->Evaluate(nullptr, table_info_->schema_).ToString();
              }
              BustubDebugLog("index_scan_executor.cpp:Next:emit", "pre-fix", "H5",
                             "Emitting visible tuple from index scan",
                             std::string("{\"rid_page\":") + std::to_string(cur_rid.GetPageId()) + ",\"rid_slot\":" +
                                 std::to_string(cur_rid.GetSlotNum()) + ",\"ts\":" + std::to_string(meta.ts_) +
                                 ",\"k0\":\"" + k0 + "\",\"pred_key\":\"" + pred_key + "\",\"k0_eq_pred\":" +
                                 ((pred_key != "null" && k0 == pred_key) ? "true" : "false") + "}");
            } catch (...) {
            }
            // #endregion agent log
            return true;
            }
        }
        
        else{
            // #region agent log
            try {
              BustubDebugLog(
                  "index_scan_executor.cpp:Next:skip_not_visible_begin", "pre-fix", "H10",
                  "Tuple from index RID is not directly visible; attempting reconstruction",
                  std::string("{\"rid_page\":") + std::to_string(cur_rid.GetPageId()) + ",\"rid_slot\":" +
                      std::to_string(cur_rid.GetSlotNum()) + ",\"meta_ts\":" + std::to_string(meta.ts_) +
                      ",\"meta_deleted\":" + (meta.is_deleted_ ? "true" : "false") + ",\"txn_read_ts\":" +
                      std::to_string(txn->GetReadTs()) + ",\"txn_temp_ts\":" +
                      std::to_string(txn->GetTransactionTempTs()) + "}");
            } catch (...) {
            }
            // #endregion agent log
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
        // #region agent log
        try {
          auto k0 = tuple->GetValue(*schema, 0).ToString();
          std::string pred_key = "null";
          if (plan_->pred_key_ != nullptr) {
            pred_key = plan_->pred_key_->Evaluate(nullptr, table_info_->schema_).ToString();
          }
          BustubDebugLog("index_scan_executor.cpp:Next:emit_reconstructed", "pre-fix", "H5",
                         "Emitting reconstructed visible tuple from index scan",
                         std::string("{\"rid_page\":") + std::to_string(cur_rid.GetPageId()) + ",\"rid_slot\":" +
                             std::to_string(cur_rid.GetSlotNum()) + ",\"ts\":" + std::to_string(meta.ts_) +
                             ",\"k0\":\"" + k0 + "\",\"pred_key\":\"" + pred_key + "\",\"k0_eq_pred\":" +
                             ((pred_key != "null" && k0 == pred_key) ? "true" : "false") + "}");
        } catch (...) {
        }
        // #endregion agent log
        return true;
    }
      }
      // #region agent log
      try {
        int64_t last_undo_ts = -1;
        if (!undo_logs.empty()) {
          last_undo_ts = undo_logs.back().ts_;
        }
        BustubDebugLog(
            "index_scan_executor.cpp:Next:skip_not_visible_end", "pre-fix", "H10",
            "Reconstruction did not yield a visible tuple; skipping RID",
            std::string("{\"rid_page\":") + std::to_string(cur_rid.GetPageId()) + ",\"rid_slot\":" +
                std::to_string(cur_rid.GetSlotNum()) + ",\"meta_ts\":" + std::to_string(meta.ts_) +
                ",\"meta_deleted\":" + (meta.is_deleted_ ? "true" : "false") + ",\"txn_read_ts\":" +
                std::to_string(txn->GetReadTs()) + ",\"found_visible\":" + (found_visible ? "true" : "false") +
                ",\"undo_cnt\":" + std::to_string(undo_logs.size()) + ",\"last_undo_ts\":" +
                std::to_string(last_undo_ts) + "}");
      } catch (...) {
      }
      // #endregion agent log
        }
    }
    return false;
 }

}  // namespace bustub

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
    while (iter_ != rids_.end()) {
        *rid = *iter_;
        auto [meta, current_tuple] = table_info_->table_->GetTuple(*rid);
        ++iter_;        
        if (!meta.is_deleted_) {
            *tuple = std::move(current_tuple);
            return true;
        }
    }
    return false;
 }

}  // namespace bustub

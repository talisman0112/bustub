//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <chrono>
#include <fstream>
#include <string>

#include "execution/executors/insert_executor.h"

namespace bustub {

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

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    :AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() { 
    child_executor_->Init();
    executed_ = false;
    
 }

auto InsertExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (executed_) {
    return false;
  }

  auto *catalog = exec_ctx_->GetCatalog();
  auto *table_info = catalog->GetTable(plan_->GetTableOid());
  auto *table_heap = table_info->table_.get();
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto *txn = exec_ctx_->GetTransaction();

  int32_t count = 0;
  Tuple child_tuple;
  RID child_rid;

  // 找到 primary key index
  IndexInfo *pk_index = nullptr;
  for (auto *idx : indexes) {
    if (idx->is_primary_key_) {
      pk_index = idx;
      break;
    }
  }

  while (child_executor_->Next(&child_tuple, &child_rid)) {
    
    // Step 1: 先检查 primary key 是否冲突（在插入 heap 之前）
    std::optional<RID> resurrect_rid;
    if (pk_index != nullptr) {
      auto key = child_tuple.KeyFromTuple(table_info->schema_, pk_index->key_schema_,
                                          pk_index->index_->GetKeyAttrs());

      std::vector<RID> result;
      pk_index->index_->ScanKey(key, &result, txn);

      for (const auto &existing_rid : result) {
        auto [existing_meta, existing_tuple] = table_heap->GetTuple(existing_rid);
        // 同一事务已删除：允许“复活并复用同一个 RID”，避免 PK index InsertEntry 失败
        if (existing_meta.is_deleted_ && existing_meta.ts_ == txn->GetTransactionTempTs()) {
          resurrect_rid = existing_rid;
          continue;
        }
    
      // 已提交的删除，且对我可见，可以重用
        if (existing_meta.is_deleted_ && existing_meta.ts_ <= txn->GetReadTs()) {
        continue;
        }

        // 未提交的其他事务 → write-write conflict
        if (existing_meta.ts_ > TXN_START_ID) {
          if (existing_meta.ts_ != txn->GetTransactionTempTs()) {
            txn->SetTainted();
            throw ExecutionException("write-write conflict on primary key");
          }
          // 自己的未提交数据，也是冲突（同一事务内重复插入相同 key）
          txn->SetTainted();
          throw ExecutionException("duplicate key in same transaction");
        }

        // 已提交且对我可见 → primary key conflict
        if (existing_meta.ts_ <= txn->GetReadTs()) {
          txn->SetTainted();
          throw ExecutionException("primary key conflict - key already exists");
        }

        // ts > read_ts 且 ts <= TXN_START_ID：在我开始后提交的，对我不可见，不冲突
      }
    }

    // Step 2: 通过检查后，要么复活旧 RID，要么插入新 tuple
    RID insert_rid;
    if (resurrect_rid.has_value()) {
      insert_rid = *resurrect_rid;
      // Only allow resurrecting the tuple that THIS txn just deleted.
      // Otherwise we might overwrite concurrent updates.
      TupleMeta meta{txn->GetTransactionTempTs(), false};
      const auto expected_ts = txn->GetTransactionTempTs();
      bool ok = table_heap->UpdateTupleInPlace(
          meta, child_tuple, insert_rid,
          [expected_ts](const TupleMeta &m, const Tuple & /*t*/, RID /*r*/) { return m.ts_ == expected_ts && m.is_deleted_; });
      if (!ok) {
        // #region agent log
        BustubDebugLog("insert_executor.cpp:Next:resurrect_cas_failed", "pre-fix", "H12",
                       "Resurrect CAS failed (tuple not deleted by this txn)",
                       std::string("{\"rid_page\":") + std::to_string(insert_rid.GetPageId()) + ",\"rid_slot\":" +
                           std::to_string(insert_rid.GetSlotNum()) + "}");
        // #endregion agent log
        txn->SetTainted();
        throw ExecutionException("resurrect conflict: tuple changed before resurrect");
      }
      // #region agent log
      std::string a_str = "";
      std::string b_str = "";
      try {
        a_str = child_tuple.GetValue(&table_info->schema_, 0).ToString();
        b_str = child_tuple.GetValue(&table_info->schema_, 1).ToString();
      } catch (...) {
      }
      BustubDebugLog("insert_executor.cpp:Next:resurrect", "pre-fix", "H12",
                     "Resurrected tuple deleted in same txn for PK insert",
                     std::string("{\"rid_page\":") + std::to_string(insert_rid.GetPageId()) + ",\"rid_slot\":" +
                         std::to_string(insert_rid.GetSlotNum()) + ",\"a\":\"" + a_str + "\",\"b\":\"" + b_str +
                         "\",\"txn_id\":" + std::to_string(txn->GetTransactionId()) + ",\"read_ts\":" +
                         std::to_string(txn->GetReadTs()) + "}");
      // #endregion agent log
    } else {
      TupleMeta meta{txn->GetTransactionTempTs(), false};
      auto insert_rid_opt = table_heap->InsertTuple(meta, child_tuple);
      if (!insert_rid_opt.has_value()) {
        continue;
      }
      insert_rid = *insert_rid_opt;
    }

    // Step 3: 插入 primary key index
    if (pk_index != nullptr) {
      auto key = child_tuple.KeyFromTuple(table_info->schema_, pk_index->key_schema_,
                                          pk_index->index_->GetKeyAttrs());

      // 如果是复活路径，PK index 里已经有该 key -> rid 映射，不需要 InsertEntry
      bool success = true;
      if (!resurrect_rid.has_value()) {
        success = pk_index->index_->InsertEntry(key, insert_rid, txn);
      }

      if (!success) {
        // 并发冲突：在 step1 检查和 step3 插入之间，其他 txn 抢先插入了
        // 回滚 heap 插入
        table_heap->UpdateTupleMeta({txn->GetTransactionTempTs(), true}, insert_rid);
        txn->SetTainted();
        // #region agent log
        BustubDebugLog("insert_executor.cpp:Next:pk_insert_failed", "pre-fix", "H12",
                       "Primary key index InsertEntry failed",
                       std::string("{\"rid_page\":") + std::to_string(insert_rid.GetPageId()) + ",\"rid_slot\":" +
                           std::to_string(insert_rid.GetSlotNum()) + "}");
        // #endregion agent log
        throw ExecutionException("concurrent insert conflict on primary key");
      }
    }

    // Step 4: 插入 secondary indexes
    for (auto *index_info : indexes) {
      if (index_info->is_primary_key_) {
        continue;
      }
      auto key = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                          index_info->index_->GetKeyAttrs());
      index_info->index_->InsertEntry(key, insert_rid, txn);
    }

    // Step 5: 记录 write set
    txn->AppendWriteSet(plan_->GetTableOid(), insert_rid);
    count++;
  }

  // 返回插入行数
  std::vector<Value> values{{TypeId::INTEGER, count}};
  *tuple = Tuple{values, &GetOutputSchema()};
  executed_ = true;
  return true;
}
}

 // namespace bustub

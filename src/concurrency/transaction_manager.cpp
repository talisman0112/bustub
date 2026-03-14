//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// transaction_manager.cpp
//
// Identification: src/concurrency/transaction_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_manager.h"

#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

#include "catalog/catalog.h"
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "concurrency/transaction.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "storage/table/tuple.h"
#include "type/type_id.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto TransactionManager::Begin(IsolationLevel isolation_level) -> Transaction * {
  std::unique_lock<std::shared_mutex> l(txn_map_mutex_);
  auto txn_id = next_txn_id_++;
  auto txn = std::make_unique<Transaction>(txn_id, isolation_level);
  auto *txn_ref = txn.get();
  txn_map_.insert(std::make_pair(txn_id, std::move(txn)));
  txn_ref->read_ts_=last_commit_ts_.load();
  running_txns_.AddTxn(txn_ref->read_ts_);
  return txn_ref;
}

auto TransactionManager::VerifyTxn(Transaction *txn) -> bool { return true; }

auto TransactionManager::Commit(Transaction *txn) -> bool {
  std::unique_lock<std::mutex> commit_lck(commit_mutex_);

  if (txn->state_ != TransactionState::RUNNING) {
    throw Exception("txn not in running state");
  }

  if (txn->GetIsolationLevel() == IsolationLevel::SERIALIZABLE) {
    if (!VerifyTxn(txn)) {
      commit_lck.unlock();
      Abort(txn);
      return false;
    }
  }

  // 1. 获取 commit timestamp
  auto commit_ts = ++last_commit_ts_;

  // 2. 遍历 write set，更新所有修改过的 tuple 的 ts
  for (const auto &[table_oid, rid_set] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    
    for (const auto &rid : rid_set) {
      // 更新 tuple meta 的 ts
      auto meta = table_info->table_->GetTupleMeta(rid);
      if (meta.ts_ == txn->GetTransactionTempTs()) {
        meta.ts_ = commit_ts;
        table_info->table_->UpdateTupleMeta(meta, rid);
      }

      // 清除 version link 的 in_progress 标志
      auto version_link = GetVersionLink(rid);
      if (version_link.has_value()) {
        version_link->in_progress_ = false;
        UpdateVersionLink(rid, version_link);
      }
    }
  }

  // 3. 设置事务状态和 commit ts
  txn->commit_ts_ = commit_ts;
  txn->state_ = TransactionState::COMMITTED;

  // 4. 更新 running_txns
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  running_txns_.UpdateCommitTs(commit_ts);
  running_txns_.RemoveTxn(txn->read_ts_);

  return true;
}

void TransactionManager::Abort(Transaction *txn) {
  if (txn->state_ != TransactionState::RUNNING && txn->state_ != TransactionState::TAINTED) {
    throw Exception("txn not in running / tainted state");
  }

  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);

  for (const auto &[table_oid, rid_set] : txn->GetWriteSets()) {
    auto table_info = catalog_->GetTable(table_oid);
    auto table_heap = table_info->table_.get();
    const auto &schema = table_info->schema_;

    for (const auto &rid : rid_set) {
      auto [meta, tuple] = table_heap->GetTuple(rid);
      auto undo_link = GetUndoLink(rid);
      if (!undo_link.has_value() || !undo_link->IsValid()) {
        // INSERT
        meta.is_deleted_ = true;
        meta.ts_ = txn->GetTransactionId(); 
        table_heap->UpdateTupleMeta(meta, rid);
        continue;
      }
      auto undo_log = GetUndoLog(*undo_link);     
      TupleMeta restored_meta;
      restored_meta.ts_ = undo_log.ts_;
      restored_meta.is_deleted_ = undo_log.is_deleted_;
      auto restored_tuple = ReconstructTuple(&schema, tuple, meta, {undo_log});
      if (restored_tuple.has_value()) {
        table_heap->UpdateTupleInPlace(restored_meta, restored_tuple.value(), rid);
      } else {
        table_heap->UpdateTupleMeta(restored_meta, rid);
      }
      std::optional<UndoLink> prev = undo_log.prev_version_.IsValid()
          ? std::make_optional(undo_log.prev_version_)
          : std::nullopt;
      UpdateVersionLink(rid, VersionUndoLink::FromOptionalUndoLink(prev), nullptr);
    }
  }
  txn->state_ = TransactionState::ABORTED;
  running_txns_.RemoveTxn(txn->read_ts_);
}


void TransactionManager::GarbageCollection() {
  timestamp_t watermark = GetWatermark();
  
  std::unordered_set<txn_id_t> active_txn_ids;
  std::vector<std::tuple<txn_id_t, int, UndoLog>> logs_to_truncate;
  
  // 需要直接清空 version link 的 RID
  std::vector<std::pair<RID, VersionUndoLink>> links_to_clear;
  
  auto table_names = catalog_->GetTableNames();
  for (const auto &table_name : table_names) {
    auto table_info = catalog_->GetTable(table_name);
    auto &table_heap = table_info->table_;
    
    for (auto iter = table_heap->MakeIterator(); !iter.IsEnd(); ++iter) {
      auto rid = iter.GetRID();
      auto [meta, tuple] = iter.GetTuple();
      
      auto version_link = GetVersionLink(rid);
      if (!version_link.has_value() || !version_link->prev_.IsValid()) {
        continue;
      }
      
      // 关键：检查 heap tuple 的 ts
      if (meta.ts_ < watermark) {
        // heap tuple 本身就 < watermark，整个 undo chain 都可以清理
        // 直接把 version link 的 prev 设为 invalid
        VersionUndoLink new_link = *version_link;
        new_link.prev_ = UndoLink{};
        links_to_clear.emplace_back(rid, new_link);
        continue;
      }
      
      // heap tuple ts >= watermark，需要遍历 undo chain
      auto undo_link = version_link->prev_;
      bool found_first_below_watermark = false;
      
      while (undo_link.IsValid()) {
        UndoLog undo_log = GetUndoLog(undo_link);
        
        if (undo_log.ts_ < watermark) {
          if (!found_first_below_watermark) {
            found_first_below_watermark = true;
            active_txn_ids.insert(undo_link.prev_txn_);
            
            if (undo_log.prev_version_.IsValid()) {
              UndoLog truncated_log = undo_log;
              truncated_log.prev_version_ = UndoLink{};
              logs_to_truncate.emplace_back(undo_link.prev_txn_, undo_link.prev_log_idx_, truncated_log);
            }
            break;
          }
        } else {
          active_txn_ids.insert(undo_link.prev_txn_);
        }
        
        undo_link = undo_log.prev_version_;
      }
    }
  }
  
  // 清空需要清理的 version link
  for (const auto &[rid, new_link] : links_to_clear) {
    UpdateVersionLink(rid, new_link);
  }
  
  // 执行截断操作
  {
    std::shared_lock<std::shared_mutex> lck(txn_map_mutex_);
    for (const auto &[txn_id, log_idx, truncated_log] : logs_to_truncate) {
      auto it = txn_map_.find(txn_id);
      if (it != txn_map_.end()) {
        it->second->ModifyUndoLog(log_idx, truncated_log);
      }
    }
  }
  
  // 清理不再需要的事务
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  
  std::vector<txn_id_t> txns_to_remove;
  for (const auto &[txn_id, txn] : txn_map_) {
    auto state = txn->GetTransactionState();
    if (state == TransactionState::COMMITTED || state == TransactionState::ABORTED) {
      if (active_txn_ids.find(txn_id) == active_txn_ids.end()) {
        txns_to_remove.push_back(txn_id);
      }
    }
  }
  
  for (auto txn_id : txns_to_remove) {
    txn_map_.erase(txn_id);
  }
}
}  // namespace bustub

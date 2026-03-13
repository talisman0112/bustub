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
  // TODO(fall2023): acquire commit ts!
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
  // TODO(fall2023): Implement the commit logic!
  std::unique_lock<std::shared_mutex> lck(txn_map_mutex_);
  // TODO(fall2023): set commit timestamp + update last committed timestamp here.
  txn->state_ = TransactionState::COMMITTED;
  auto commit_ts = ++last_commit_ts_;
  txn->commit_ts_=commit_ts;
  running_txns_.UpdateCommitTs(txn->commit_ts_);
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


void TransactionManager::GarbageCollection() { UNIMPLEMENTED("not implemented"); }

}  // namespace bustub

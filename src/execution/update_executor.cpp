//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <chrono>
#include <fstream>
#include <string>

#include "execution/executors/update_executor.h"
#include "catalog/catalog.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"
#include "execution/execution_common.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
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

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
}
void UpdateExecutor::Init() {
  child_executor_->Init();
  is_updated_ = false;
}

// Next() 只执行一次：
// 1) 拉取子执行器给出的所有候选行（这些行已由子计划筛选，且对本事务可见）；
// 2) 对每一行执行 MVCC 更新流程：冲突检测 -> undo/version link 维护 -> 条件写入 -> 索引维护；
// 3) 返回一行 [count] 表示受影响行数；下一次调用直接返回 false。
auto UpdateExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (is_updated_) {
    return false;
  }

  auto catalog = exec_ctx_->GetCatalog();
  auto table_info = catalog->GetTable(plan_->table_oid_);
  auto table_heap = table_info->table_.get();
  auto txn = exec_ctx_->GetTransaction();
  auto txn_mgr = exec_ctx_->GetTransactionManager();
  auto table_oid = plan_->table_oid_;
  auto indexes = catalog->GetTableIndexes(table_info->name_);
  auto column_count = table_info->schema_.GetColumnCount();
  // 判断某个索引键是否变化：
  // - 变化：需要 delete(old_key) + insert(new_key)
  // - 不变：索引项可复用，无需更新该索引
  // 注意：Tuple::KeyFromTuple 在本项目是非 const 成员函数，因此参数不能是 const Tuple&。
  auto key_changed = [](Tuple &old_tuple, Tuple &new_tuple, const Schema &table_schema, const Schema &key_schema,
                        const std::vector<uint32_t> &key_attrs) -> bool {
    auto old_key = old_tuple.KeyFromTuple(table_schema, key_schema, key_attrs);
    auto new_key = new_tuple.KeyFromTuple(table_schema, key_schema, key_attrs);
    for (uint32_t i = 0; i < key_schema.GetColumnCount(); i++) {
      if (!old_key.GetValue(&key_schema, i).CompareExactlyEquals(new_key.GetValue(&key_schema, i))) {
        return true;
      }
    }
    return false;
  };
  
  Tuple child_tuple;
  RID child_rid;
  int32_t count = 0;
  
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // ---------- 单行更新开始 ----------
    // child 已给出要更新的目标行；但真正写回前，仍要读取当前 heap 版本并做并发校验。
    fmt::println(stderr, "UpdateExecutor: processing rid={}", child_rid.ToString());
    auto old_meta = table_heap->GetTupleMeta(child_rid);
    auto old_tuple = table_heap->GetTuple(child_rid).second;
    // #region agent log
    BustubDebugLog("update_executor.cpp:Next:begin_rid", "pre-fix", "H3",
                   "Begin updating rid",
                   std::string("{\"rid_page\":") + std::to_string(child_rid.GetPageId()) + ",\"rid_slot\":" +
                       std::to_string(child_rid.GetSlotNum()) + ",\"old_ts\":" + std::to_string(old_meta.ts_) +
                       ",\"read_ts\":" + std::to_string(txn->GetReadTs()) + "}");
    // #endregion agent log
    
    // 基于 target_expressions 和 child 输出行计算 new_tuple（候选更新结果）。
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    Tuple new_tuple(new_values, &table_info->schema_);

    // ---------- 分流 A：主键是否变化 ----------
    // 主键变化不走“普通原地更新”语义，而是走“逻辑 delete(old) + insert(new)”语义：
    // - 仍复用同一 RID；
    // - 通过 is_deleted 翻转 + 新值写入完成“删后复活”；
    // - 同步维护所有受影响索引。
    IndexInfo *pk_index_info = nullptr;
    for (auto *idx : indexes) {
      if (idx->is_primary_key_) {
        pk_index_info = idx;
        break;
      }
    }
    const bool pk_changed = pk_index_info != nullptr &&
                            key_changed(old_tuple, new_tuple, table_info->schema_, pk_index_info->key_schema_,
                                        pk_index_info->index_->GetKeyAttrs());
    if (pk_changed) {
      // [主键变化-前置冲突检查]
      // 1) old_meta.ts_ > TXN_START_ID: 行当前是某事务 temp ts（未提交），继续写会和对方写写冲突；
      // 2) old_meta.ts_ > read_ts: 行在我快照之后已被他人提交更新，属于丢失更新冲突。
      if (old_meta.ts_ > TXN_START_ID) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict: uncommitted txn");
      }
      if (old_meta.ts_ > txn->GetReadTs()) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict: committed after my read_ts");
      }

      // [主键变化-同事务二次更新]
      // 若行已经是本事务 temp ts，说明本事务先前已占有该行：
      // - 不再新增 undo 节点（避免同事务重复改导致 undo 膨胀）；
      // - 直接在本事务版本上 CAS 更新 tuple；
      // - 再按 key 变化更新索引。
      if (old_meta.ts_ == txn->GetTransactionTempTs()) {
        TupleMeta new_meta{txn->GetTransactionTempTs(), false};
        bool updated = table_heap->UpdateTupleInPlace(
            new_meta, new_tuple, child_rid,
            [txn](const TupleMeta &meta, const Tuple & /*table*/, RID /*rid*/) { return meta.ts_ == txn->GetTransactionTempTs(); });
        if (!updated) {
          txn->SetTainted();
          throw ExecutionException("Write-write conflict: tuple changed before update");
        }
        txn->AppendWriteSet(table_oid, child_rid);

        for (auto *index_info : indexes) {
          if (!key_changed(old_tuple, new_tuple, table_info->schema_, index_info->key_schema_,
                           index_info->index_->GetKeyAttrs())) {
            continue;
          }
          auto old_key = old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
          auto new_key = new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
          index_info->index_->DeleteEntry(old_key, child_rid, txn);
          index_info->index_->InsertEntry(new_key, child_rid, txn);
        }

        count++;
        continue;
      }

      // [主键变化-步骤1/5] 构造 full-row undo。
      // 主键变化可能影响整行身份语义，因此记录“整行旧值”便于回滚恢复。
      // 同时读取原链头：若 in_progress=true，代表其他事务正在改这条 version link，直接冲突退出。
      auto version_link = txn_mgr->GetVersionLink(child_rid);
      UndoLink prev_undo_link;
      if (version_link.has_value()) {
        if (version_link->in_progress_) {
          txn->SetTainted();
          throw ExecutionException("Write-write conflict: in_progress");
        }
        prev_undo_link = version_link->prev_;
      }

      std::vector<bool> modified_fields(column_count, true);
      std::vector<Column> undo_columns;
      std::vector<Value> undo_values;
      undo_columns.reserve(column_count);
      undo_values.reserve(column_count);
      for (uint32_t i = 0; i < column_count; i++) {
        undo_columns.push_back(table_info->schema_.GetColumn(i));
        undo_values.push_back(old_tuple.GetValue(&table_info->schema_, i));
      }
      Schema partial_schema(undo_columns);
      Tuple partial_tuple(undo_values, &partial_schema);
      UndoLog undo_log;
      undo_log.is_deleted_ = old_meta.is_deleted_;
      undo_log.ts_ = old_meta.ts_;
      undo_log.modified_fields_ = modified_fields;
      undo_log.tuple_ = partial_tuple;
      undo_log.prev_version_ = prev_undo_link;
      auto new_undo_link = txn->AppendUndoLog(undo_log);

      // [主键变化-步骤2/5] CAS 更新 version link：
      // - prev_ 指向新 append 的 undo；
      // - in_progress=true，声明“这条链正在被我修改”；
      // - check 保证只有在旧链不存在或旧链非 in_progress 时才更新成功。
      VersionUndoLink new_version_link;
      new_version_link.prev_ = new_undo_link;
      new_version_link.in_progress_ = true;
      bool vlink_ok = txn_mgr->UpdateVersionLink(child_rid, new_version_link, [](std::optional<VersionUndoLink> old) {
        return !old.has_value() || !old->in_progress_;
      });
      if (!vlink_ok) {
        txn->SetTainted();
        throw ExecutionException("Write-write conflict: in_progress");
      }

      // [主键变化-步骤3/5] 逻辑删除旧值：
      // 将元数据置为 (temp_ts, is_deleted=true)，并用 expected_ts 做条件检查，
      // 确保删除的是“我最初读取到的那一版”，防止并发覆盖。
      {
        TupleMeta del_meta{txn->GetTransactionTempTs(), true};
        const auto expected_ts = old_meta.ts_;
        bool deleted = table_heap->UpdateTupleInPlace(
            del_meta, old_tuple, child_rid,
            [expected_ts](const TupleMeta &meta, const Tuple & /*table*/, RID /*rid*/) { return meta.ts_ == expected_ts; });
        if (!deleted) {
          txn->SetTainted();
          throw ExecutionException("Write-write conflict: tuple changed before delete");
        }
      }

      // [主键变化-步骤4/5] 在同一 RID 复活新值：
      // 条件要求“当前仍是我的 temp_ts 且 is_deleted=true”，
      // 证明步骤3到步骤4之间没有被其他事务插入或覆盖。
      {
        TupleMeta ins_meta{txn->GetTransactionTempTs(), false};
        bool resurrected =
            table_heap->UpdateTupleInPlace(ins_meta, new_tuple, child_rid,
                                           [txn](const TupleMeta &meta, const Tuple & /*table*/, RID /*rid*/) {
                                             return meta.ts_ == txn->GetTransactionTempTs() && meta.is_deleted_;
                                           });
        if (!resurrected) {
          txn->SetTainted();
          throw ExecutionException("Write-write conflict: resurrect CAS failed");
        }
      }

      // [主键变化-步骤5/5] 维护所有变化索引：
      // 对 key 发生变化的索引执行 delete(old)+insert(new)，保证索引视图与 heap 一致。
      for (auto *index_info : indexes) {
        if (!key_changed(old_tuple, new_tuple, table_info->schema_, index_info->key_schema_,
                         index_info->index_->GetKeyAttrs())) {
          continue;
        }
        auto old_key =
            old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        auto new_key =
            new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->DeleteEntry(old_key, child_rid, txn);
        index_info->index_->InsertEntry(new_key, child_rid, txn);
      }

      txn->AppendWriteSet(table_oid, child_rid);
      count++;
      continue;
    }
    
    // ---------- 分流 B：主键未变（普通更新路径）----------
    if (old_meta.ts_ == txn->GetTransactionTempTs()) {
      // Case 1: old_meta.ts_ == my temp ts（同事务重复更新同一行）
      // 目标：不新增 undo 节点，而是“扩大”已有 undo 覆盖范围：
      // - modified_fields 做 OR 合并（旧改动 + 本轮改动）；
      // - undo payload 始终保存“事务首次修改前”的原始值，保证一次回滚可恢复。
      auto version_link = txn_mgr->GetVersionLink(child_rid);
      
      if (version_link.has_value() && version_link->prev_.IsValid()) {
        auto undo_link = version_link->prev_;
        auto old_undo_log = txn_mgr->GetUndoLog(undo_link);
        
        // 借助旧 undo 重建事务首次修改前的原始 tuple。
        auto reconstructed = ReconstructTuple(&table_info->schema_, old_tuple, old_meta, {old_undo_log});
        
        if (reconstructed.has_value()) {
          Tuple original_tuple = reconstructed.value();
          
          // 关键：合并修改位图（旧位图 OR 本轮变化列）。
          std::vector<bool> new_modified_fields = old_undo_log.modified_fields_;
          
          // 确保大小正确
          if (new_modified_fields.size() < column_count) {
            new_modified_fields.resize(column_count, false);
          }
          
          // 识别本轮新增变化列并并入位图。
          for (uint32_t i = 0; i < column_count; i++) {
            auto heap_value = old_tuple.GetValue(&table_info->schema_, i);
            auto new_value = new_tuple.GetValue(&table_info->schema_, i);
            if (!heap_value.CompareExactlyEquals(new_value)) {
              new_modified_fields[i] = true;
            }
          }
          
          // 按合并位图重建 undo payload：保存原始值，不保存中间值。
          std::vector<Value> new_undo_values;
          std::vector<Column> new_undo_columns;
          
          for (uint32_t i = 0; i < column_count; i++) {
            if (new_modified_fields[i]) {
              new_undo_values.push_back(original_tuple.GetValue(&table_info->schema_, i));
              new_undo_columns.push_back(table_info->schema_.GetColumn(i));
            }
          }
          
          // 用新 payload 覆盖旧 undo，prev_version 保持不变。
          Schema new_partial_schema(new_undo_columns);
          Tuple new_partial_tuple(new_undo_values, &new_partial_schema);
          
          UndoLog new_undo_log;
          new_undo_log.is_deleted_ = old_undo_log.is_deleted_;
          new_undo_log.ts_ = old_undo_log.ts_;
          new_undo_log.modified_fields_ = new_modified_fields;
          new_undo_log.tuple_ = new_partial_tuple;
          new_undo_log.prev_version_ = old_undo_log.prev_version_;
          
          txn->ModifyUndoLog(undo_link.prev_log_idx_, new_undo_log);
        }
      }
      // 若该行来源于本事务 insert，可能没有可改写的历史 undo，跳过即可。

    } else if (old_meta.ts_ > TXN_START_ID) {
      // Case 2: 行当前是其他事务 temp 版本（未提交）=> 写写冲突。
      txn->SetTainted();
      throw ExecutionException("Write-write conflict: uncommitted txn");
      
    } else if (old_meta.ts_ > txn->GetReadTs()) {
      // Case 3: 行在我 read_ts 之后被他人提交更新 => 丢失更新冲突。
      txn->SetTainted();
      throw ExecutionException("Write-write conflict: committed after my read_ts");
    } else {
      // Case 4: 首次修改“已提交且对我可见”的版本
      // 执行策略：
      // - 仅记录变化列旧值（partial undo，降低空间）；
      // - 继承当前 version link 链头；
      // - CAS 更新链头并置 in_progress，独占这条链直到提交。
      std::vector<bool> modified_fields(column_count, false);
      std::vector<Value> undo_values;
      std::vector<Column> undo_columns;
      
      for (uint32_t i = 0; i < column_count; i++) {
        auto old_value = old_tuple.GetValue(&table_info->schema_, i);
        auto new_value = new_tuple.GetValue(&table_info->schema_, i);
        if (!old_value.CompareExactlyEquals(new_value)) {
          modified_fields[i] = true;
          undo_values.push_back(old_value);
          undo_columns.push_back(table_info->schema_.GetColumn(i));
        }
      }
      
      // 读取旧链头；若 old.in_progress=true，说明别人正改链，冲突退出。
      auto version_link = txn_mgr->GetVersionLink(child_rid);
      UndoLink prev_undo_link;
      if (version_link.has_value()) {
        if(version_link->in_progress_){
        txn->SetTainted();
        throw ExecutionException("Write-write conflict: in_progress");
        }
        prev_undo_link = version_link->prev_; 
      }
      
      // 将变化列旧值编码成 partial tuple，作为 undo payload。
      Schema partial_schema(undo_columns);
      Tuple partial_tuple(undo_values, &partial_schema);
      
      UndoLog undo_log;
      undo_log.is_deleted_ = old_meta.is_deleted_;
      undo_log.ts_ = old_meta.ts_;
      undo_log.modified_fields_ = modified_fields;
      undo_log.tuple_ = partial_tuple;
      undo_log.prev_version_ = prev_undo_link;
      
      auto new_undo_link = txn->AppendUndoLog(undo_log);

      // CAS 更新 version link：
      // - prev_ 指向新 undo；
      // - in_progress=true；提交时由 TransactionManager 统一清除。
      VersionUndoLink new_version_link;
    new_version_link.prev_ = new_undo_link;
    new_version_link.in_progress_ = true;
    bool success = txn_mgr->UpdateVersionLink(child_rid, new_version_link,
    [](std::optional<VersionUndoLink> old) {
        return !old.has_value() || !old->in_progress_;
    });
    if (!success) {
    txn->SetTainted();
    throw ExecutionException("Write-write conflict: in_progress");
    }
      
    }
    
    // 统一写回步骤（两个分流最终都会走到这里）：
    // - 把 tuple 内容更新为 new_tuple；
    // - 把 meta.ts_ 置为本事务 temp ts；
    // - 通过 expected_ts 条件写，确保仍是我决策时看到的版本（最后一道 CAS 防线）。
    TupleMeta new_meta{txn->GetTransactionTempTs(), false};
    const auto expected_ts = old_meta.ts_;
    bool updated = table_heap->UpdateTupleInPlace(
        new_meta, new_tuple, child_rid,
        [expected_ts](const TupleMeta &meta, const Tuple & /*table*/, RID /*rid*/) { return meta.ts_ == expected_ts; });
    if (!updated) {
      // #region agent log
      BustubDebugLog("update_executor.cpp:Next:cas_failed", "pre-fix", "H3",
                     "Conditional in-place update failed (ts changed)",
                     std::string("{\"rid_page\":") + std::to_string(child_rid.GetPageId()) + ",\"rid_slot\":" +
                         std::to_string(child_rid.GetSlotNum()) + ",\"expected_ts\":" + std::to_string(expected_ts) +
                         ",\"read_ts\":" + std::to_string(txn->GetReadTs()) + "}");
      // #endregion agent log
      txn->SetTainted();
      throw ExecutionException("Write-write conflict: tuple changed before update");
    }
    // 记录 write set：
    // Commit 会遍历该集合，把 temp ts 刷成 commit ts，并清理 version link 的 in_progress。
    txn->AppendWriteSet(table_oid, child_rid);
    // #region agent log
    {
      auto after_meta = table_heap->GetTupleMeta(child_rid);
      std::string a_str = "";
      std::string b_str = "";
      try {
        a_str = new_tuple.GetValue(&table_info->schema_, 0).ToString();
        b_str = new_tuple.GetValue(&table_info->schema_, 1).ToString();
      } catch (...) {
      }
      BustubDebugLog("update_executor.cpp:Next:updated_inplace", "pre-fix", "H3",
                     "Updated tuple in-place (CAS ok)",
                     std::string("{\"rid_page\":") + std::to_string(child_rid.GetPageId()) + ",\"rid_slot\":" +
                         std::to_string(child_rid.GetSlotNum()) + ",\"new_ts\":" + std::to_string(new_meta.ts_) +
                         ",\"after_ts\":" + std::to_string(after_meta.ts_) + ",\"a\":\"" + a_str + "\",\"b\":\"" +
                         b_str + "\",\"txn_id\":" + std::to_string(txn->GetTransactionId()) + ",\"read_ts\":" +
                         std::to_string(txn->GetReadTs()) + "}");
    }
    // #endregion agent log
    // 索引收尾（主键未变路径下也要处理二级索引键变化）：
    // 仅当 key 变化时做 delete+insert；in_progress 只在 Commit 中清理，不在 executor 内清理。
    for (auto *index_info : indexes) {
    auto old_key = old_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                          index_info->index_->GetKeyAttrs());
    auto new_key = new_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_,
                                          index_info->index_->GetKeyAttrs());
    
    // 检查 key 是否变了
    bool key_changed = false;
    for (uint32_t i = 0; i < index_info->key_schema_.GetColumnCount(); i++) {
        if (!old_key.GetValue(&index_info->key_schema_, i)
                .CompareExactlyEquals(new_key.GetValue(&index_info->key_schema_, i))) {
            key_changed = true;
            break;
        }
    }
    
    if (key_changed) {
        index_info->index_->DeleteEntry(old_key, child_rid, txn);
        index_info->index_->InsertEntry(new_key, child_rid, txn);
    }
}
    
    count++;
  }
  fmt::println(stderr, "UpdateExecutor: total count={}", count);
  // 执行结束：返回 UPDATE 影响行数（单列 INTEGER），并标记为已输出结果。
  std::vector<Value> values{Value(TypeId::INTEGER, count)};
  *tuple = Tuple(values, &GetOutputSchema());
  is_updated_ = true;
  return true;
}

}  // namespace bustub

#include "execution/execution_common.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/macros.h"
#include "concurrency/transaction_manager.h"
#include "fmt/core.h"
#include "storage/table/table_heap.h"
#include "type/value.h"
#include "type/value_factory.h"

namespace bustub {

auto ReconstructTuple(const Schema *schema, const Tuple &base_tuple, const TupleMeta &base_meta,
                      const std::vector<UndoLog> &undo_logs) -> std::optional<Tuple> {
    std::vector<Value> values;
  for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
    values.push_back(base_tuple.GetValue(schema, i));
  }
  bool is_deleted = base_meta.is_deleted_;
  for(const auto &undo_log:undo_logs){
    is_deleted = undo_log.is_deleted_;
    std::vector<Column> partial_columns;
    for (uint32_t i = 0; i < schema->GetColumnCount(); i++){
      if(undo_log.modified_fields_[i]){
        partial_columns.push_back(schema->GetColumn(i));
      }
    }
    Schema partial_schema(partial_columns);
    uint32_t partial_idx = 0;  
    for (uint32_t original_idx = 0; original_idx < schema->GetColumnCount(); original_idx++) {
    if (undo_log.modified_fields_[original_idx]) {
    values[original_idx] = undo_log.tuple_.GetValue(&partial_schema, partial_idx);
    partial_idx++;
    }
    }
  }
  if (is_deleted) {
    return std::nullopt;
  }
  return Tuple{values, schema};
}

void TxnMgrDbg(const std::string &info, TransactionManager *txn_mgr, const TableInfo *table_info,
               TableHeap *table_heap) {
  // 辅助函数：格式化时间戳/事务ID
  auto format_ts = [](timestamp_t ts) -> std::string {
    if (ts >= TXN_START_ID) {
      return "txn" + std::to_string(ts ^ TXN_START_ID);
    }
    return std::to_string(ts);
  };

  fmt::println(stderr, "\n--- DEBUG: {} ---", info);

  auto iter = table_heap->MakeIterator();
  while (!iter.IsEnd()) {
    RID rid = iter.GetRID();
    auto [meta, tuple] = iter.GetTuple();
    auto const &full_schema = table_info->schema_;

    // 1. 打印 Base Tuple (TableHeap 中的当前值)
    std::string ts_str = format_ts(meta.ts_);
    std::string tuple_str = meta.is_deleted_ ? "<del marker>" : tuple.ToString(&full_schema);
    fmt::println(stderr, "RID={}/{} ts={} tuple={}", rid.GetPageId(), rid.GetSlotNum(), ts_str, tuple_str);

    // 2. 遍历 UndoLog 版本链
    std::optional<UndoLink> link = txn_mgr->GetUndoLink(rid);
    while (link.has_value() && link->IsValid()) {
      auto log = txn_mgr->GetUndoLog(*link);
      
      std::string log_data_str;
      if (log.is_deleted_) {
        log_data_str = "<del>";
      } else {
        // 关键逻辑：解析 Partial Tuple
        std::vector<uint32_t> cols;
        for (uint32_t i = 0; i < full_schema.GetColumnCount(); i++) {
          if (log.modified_fields_[i]) {
            cols.push_back(i);
          }
        }
        
        // 构建 Partial Schema 以读取 log.tuple_ 中的值
        auto partial_schema = Schema::CopySchema(&full_schema, cols);
        
        log_data_str = "(";
        uint32_t partial_idx = 0;
        for (uint32_t i = 0; i < full_schema.GetColumnCount(); i++) {
          if (log.modified_fields_[i]) {
            Value val = log.tuple_.GetValue(&partial_schema, partial_idx++);
            log_data_str += val.ToString();
          } else {
            log_data_str += "_"; // 未修改的列显示下划线
          }
          if (i < full_schema.GetColumnCount() - 1) {
            log_data_str += ", ";
          }
        }
        log_data_str += ")";
      }

      // 打印这一行版本信息
      fmt::println(stderr, "  txn{}@{} {} ts={}", 
                   link->prev_txn_ ^ TXN_START_ID, 
                   link->prev_log_idx_, 
                   log_data_str, 
                   format_ts(log.ts_));

      // 移动到下一个版本
      link = log.prev_version_;
    }
    
    ++iter;
  }
  fmt::println(stderr, "--- END DEBUG ---\n");
}

}  // namespace bustub

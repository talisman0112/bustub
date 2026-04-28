#include <vector>
#include <chrono>
#include <fstream>
#include <string>

#include "storage/index/extendible_hash_table_index.h"

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
/*
 * Constructor
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_INDEX_TYPE::ExtendibleHashTableIndex(std::unique_ptr<IndexMetadata> &&metadata,
                                                BufferPoolManager *buffer_pool_manager,
                                                const HashFunction<KeyType> &hash_fn)
    : Index(std::move(metadata)),
      comparator_(GetMetadata()->GetKeySchema()),
      container_(GetMetadata()->GetName(), buffer_pool_manager, comparator_, hash_fn) {}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_INDEX_TYPE::InsertEntry(const Tuple &key, RID rid, Transaction *transaction) -> bool {
  // construct insert index key
  KeyType index_key;
  index_key.SetFromKey(key);

  return container_.Insert(index_key, rid, transaction);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_INDEX_TYPE::DeleteEntry(const Tuple &key, RID rid, Transaction *transaction) {
  // construct delete index key
  KeyType index_key;
  index_key.SetFromKey(key);

  container_.Remove(index_key, transaction);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_INDEX_TYPE::ScanKey(const Tuple &key, std::vector<RID> *result, Transaction *transaction) {
  // construct scan index key
  KeyType index_key;
  index_key.SetFromKey(key);

  container_.GetValue(index_key, result, transaction);
  // #region agent log
  std::string rid0 = "null";
  std::string rid1 = "null";
  if (result != nullptr) {
    if (result->size() >= 1) {
      rid0 = std::to_string((*result)[0].GetPageId()) + ":" + std::to_string((*result)[0].GetSlotNum());
    }
    if (result->size() >= 2) {
      rid1 = std::to_string((*result)[1].GetPageId()) + ":" + std::to_string((*result)[1].GetSlotNum());
    }
  }
  BustubDebugLog("extendible_hash_table_index.cpp:ScanKey", "pre-fix", "H4",
                 "ScanKey returned rids",
                 std::string("{\"result_size\":") + std::to_string(result->size()) + ",\"rid0\":\"" + rid0 +
                     "\",\"rid1\":\"" + rid1 + "\"}");
  // #endregion agent log
}
template class ExtendibleHashTableIndex<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTableIndex<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTableIndex<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTableIndex<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTableIndex<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

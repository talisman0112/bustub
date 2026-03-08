//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"


namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
  auto header_guard = bpm_->NewPageGuarded(&header_page_id_);
  auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
  header_page->Init(header_max_depth_);
}


template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Hash(K key) const -> uint32_t {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::MigrateEntries(
    ExtendibleHTableBucketPage<K, V, KC> *old_bucket,
    ExtendibleHTableBucketPage<K, V, KC> *new_bucket,
    uint32_t new_bucket_idx, 
    uint32_t local_depth_mask) {
  std::vector<std::pair<K, V>> entries;
  for (uint32_t i = 0; i < old_bucket->Size(); ++i) {
    entries.push_back(old_bucket->EntryAt(i));
  }
  
  old_bucket->Init(bucket_max_size_);
  
  for (const auto &entry : entries) {
    uint32_t h = Hash(entry.first);
    if ((h & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
      new_bucket->Insert(entry.first, entry.second, cmp_);
    } else {
      old_bucket->Insert(entry.first, entry.second, cmp_);
    }
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
  uint32_t hash=Hash(key);
  auto header_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
  uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
  page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
  if (directory_page_id == INVALID_PAGE_ID) {
    return false; 
}
  auto directory_guard = bpm_->FetchPageRead(directory_page_id);
  auto directory_page = directory_guard.As<ExtendibleHTableDirectoryPage>();
  uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    return false; 
}
  auto bucket_guard=bpm_->FetchPageRead(bucket_page_id);
  auto bucket_page = bucket_guard.As<ExtendibleHTableBucketPage<K, V, KC>>();
  V value;
  bool find=bucket_page->Lookup(key,value,cmp_);
  if (find) {
    result->clear();
    result->push_back(value);
}
return find;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);

  // --- 步骤 1: 访问 Header Page ---
  // 我们只在这里拿 Header 的锁，拿到 Directory Page ID 后立即释放
  page_id_t directory_page_id = INVALID_PAGE_ID;
  {
    auto header_guard = bpm_->FetchPageWrite(header_page_id_);
    auto header_page = header_guard.AsMut<ExtendibleHTableHeaderPage>();
    uint32_t directory_idx = header_page->HashToDirectoryIndex(hash);
    directory_page_id = header_page->GetDirectoryPageId(directory_idx);

    if (directory_page_id == INVALID_PAGE_ID) {
    header_guard.Drop();
    return InsertToNewDirectory(header_page, directory_idx, hash, key, value);
  }
  } 

  auto directory_guard = bpm_->FetchPageWrite(directory_page_id);
  auto directory_page = directory_guard.AsMut<ExtendibleHTableDirectoryPage>();

  while (true) {
    uint32_t bucket_idx = directory_page->HashToBucketIndex(hash);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);

    if (bucket_page_id == INVALID_PAGE_ID) {
      return InsertToNewBucket(directory_page, bucket_idx, key, value);
    }

    // --- 步骤 3: 锁定 Bucket Page ---
    auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
    auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();

    // 1. 检查 Key 是否已存在 (如果禁止重复 Key)
    V temp_v;
    if (bucket_page->Lookup(key, temp_v, cmp_)) {
      return false;
    }

    // 2. 如果桶没满，直接插入并闪人
    if (!bucket_page->IsFull()) {
      return bucket_page->Insert(key, value, cmp_);
    }

    // --- 步骤 4: 处理桶溢出 (分裂逻辑) ---
    
    // 检查是否达到最大深度限制 (防止无限分裂)
    if (directory_page->GetLocalDepth(bucket_idx) >= directory_max_depth_) {
      return false; 
    }

    // 情况 A: 需要增加 Global Depth
    if (directory_page->GetLocalDepth(bucket_idx) == directory_page->GetGlobalDepth()) {
      directory_page->IncrGlobalDepth();
    }
    // 情况 B: 桶分裂，创建新桶
    page_id_t new_bucket_page_id = INVALID_PAGE_ID;
    auto new_bucket_guard_node = bpm_->NewPageGuarded(&new_bucket_page_id);
    if (new_bucket_page_id == INVALID_PAGE_ID) return false;
    auto new_bucket_guard = new_bucket_guard_node.UpgradeWrite();
    auto new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
    new_bucket_page->Init(bucket_max_size_);
    directory_page->IncrLocalDepth(bucket_idx);
    uint32_t new_local_depth = directory_page->GetLocalDepth(bucket_idx);
    uint32_t split_image_idx = directory_page->GetSplitImageIndex(bucket_idx);
    UpdateDirectoryMapping(directory_page, split_image_idx, new_bucket_page_id, new_local_depth, 
                           directory_page->GetLocalDepthMask(bucket_idx));
    MigrateEntries(bucket_page, new_bucket_page, split_image_idx, directory_page->GetLocalDepthMask(bucket_idx));
    // --- 关键点：分裂完成后 ---
    // 释放当前 Bucket 的锁，进入下一轮 while 循环
    // 为什么要重新循环？因为分裂后，当前的 key 依然可能让新桶再次溢出（虽然概率小），
    // 且我们需要重新定位 key 该去 split_image_idx 还是原来的 bucket_idx。
    bucket_guard.Drop();
    new_bucket_guard.Drop();
  }
}


template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {
  if (directory_idx >= header->MaxSize()) {
    return false;
  }                                                            
  page_id_t new_directory_page_id = INVALID_PAGE_ID;
  auto new_directory_guard = bpm_->NewPageGuarded(&new_directory_page_id).UpgradeWrite();
  if (new_directory_page_id == INVALID_PAGE_ID) {
    return false; 
  }
  auto *new_directory_page = new_directory_guard.AsMut<ExtendibleHTableDirectoryPage>();
  new_directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx, new_directory_page_id);
  return InsertToNewBucket(new_directory_page, new_directory_page->HashToBucketIndex(hash), key, value);
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_guard = bpm_->NewPageGuarded(&new_bucket_page_id).UpgradeWrite();
  if (new_bucket_page_id == INVALID_PAGE_ID) {
    return false; 
  }
  auto *new_bucket_page = new_bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  new_bucket_page->Init(bucket_max_size_);
  for (uint32_t i = 0; i < directory->Size(); ++i) {
    directory->SetBucketPageId(i, new_bucket_page_id);
    directory->SetLocalDepth(i, 0);
  }
  return new_bucket_page->Insert(key, value, cmp_);
}

template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(
    ExtendibleHTableDirectoryPage *directory,
    uint32_t new_bucket_idx, 
    page_id_t new_bucket_page_id,
    uint32_t new_local_depth,
    uint32_t local_depth_mask) {
  uint32_t old_local_depth_mask = local_depth_mask >> 1;
  uint32_t shared_suffix = new_bucket_idx & old_local_depth_mask;
  for (uint32_t i = 0; i < directory->Size(); ++i) {
    if ((i & old_local_depth_mask) == shared_suffix) {
      directory->SetLocalDepth(i, new_local_depth);
      if ((i & local_depth_mask) == (new_bucket_idx & local_depth_mask)) {
        directory->SetBucketPageId(i, new_bucket_page_id);
      } 
    }
  }
}



/*****************************************************************************
   * REMOVE
   *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
  uint32_t hash = Hash(key);
  
  page_id_t dir_page_id = INVALID_PAGE_ID;
  {
    auto header_guard = bpm_->FetchPageRead(header_page_id_);
    auto header_page = header_guard.As<ExtendibleHTableHeaderPage>();
    dir_page_id = header_page->GetDirectoryPageId(
        header_page->HashToDirectoryIndex(hash));
  }
  
  if (dir_page_id == INVALID_PAGE_ID) return false;
  
  auto dir_guard = bpm_->FetchPageWrite(dir_page_id);
  auto dir_page = dir_guard.AsMut<ExtendibleHTableDirectoryPage>();
  
  uint32_t bucket_idx = dir_page->HashToBucketIndex(hash);
  page_id_t bucket_page_id = dir_page->GetBucketPageId(bucket_idx);
  
  if (bucket_page_id == INVALID_PAGE_ID) return false;
  
  auto bucket_guard = bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page = bucket_guard.AsMut<ExtendibleHTableBucketPage<K, V, KC>>();
  
  if (!bucket_page->Remove(key, cmp_)) {
    return false;
  }
  
  // 合并空桶
  while (bucket_page->IsEmpty() && dir_page->GetLocalDepth(bucket_idx) > 0) {
    uint32_t split_image_idx = dir_page->GetSplitImageIndex(bucket_idx);
    
    if (dir_page->GetLocalDepth(split_image_idx) != dir_page->GetLocalDepth(bucket_idx)) {
      break;
    }
    
    page_id_t split_image_page_id = dir_page->GetBucketPageId(split_image_idx);
    
    for (uint32_t i = 0; i < dir_page->Size(); ++i) {
      if (dir_page->GetBucketPageId(i) == bucket_page_id) {
        dir_page->SetBucketPageId(i, split_image_page_id);
        dir_page->DecrLocalDepth(i);
      } else if (dir_page->GetBucketPageId(i) == split_image_page_id) {
        dir_page->DecrLocalDepth(i);
      }
    }
    
    bpm_->DeletePage(bucket_page_id);
    break;
  }
  
  while (dir_page->CanShrink()) {
    dir_page->DecrGlobalDepth();
  }
  
  return true;
}


template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

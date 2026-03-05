//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  BUSTUB_ASSERT(max_depth <= HTABLE_HEADER_MAX_DEPTH, "max_depth too large!");
  //检查
  for(page_id_t &page_id:directory_page_ids_){
    page_id=INVALID_PAGE_ID;
  }
  max_depth_=max_depth;
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t { 
  if(max_depth_==0){
    return 0;
  }
  uint32_t index = hash >> (32 - max_depth_);
  return index;
 }

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t {
   if (directory_idx >= HTABLE_HEADER_ARRAY_SIZE) {
      return INVALID_PAGE_ID; 
  }
  return directory_page_ids_[directory_idx];
  }

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
 if (directory_idx >= HTABLE_HEADER_ARRAY_SIZE) {
      return; 
  }
  directory_page_ids_[directory_idx] = directory_page_id;
}

auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { return 1<<max_depth_; }

}  // namespace bustub

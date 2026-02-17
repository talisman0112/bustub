//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    Page &old_page = pages_[frame_id];
    if (old_page.IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, old_page.GetData(), old_page.GetPageId(), std::move(promise)});
      future.get();
    }
    page_table_.erase(old_page.GetPageId());
  }
  
  *page_id = AllocatePage();
  Page &new_page = pages_[frame_id];
  new_page.ResetMemory();
  new_page.page_id_ = *page_id;
  new_page.is_dirty_ = false;
  new_page.pin_count_ = 1;
  
  page_table_[*page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  return &new_page;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::scoped_lock<std::mutex> lock(latch_);
  
  if (auto it = page_table_.find(page_id); it != page_table_.end()) {
    Page &page = pages_[it->second];
    page.pin_count_++;
    replacer_->RecordAccess(it->second);
    replacer_->SetEvictable(it->second, false);
    return &page;
  }
  
  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else {
    if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }
    Page &old_page = pages_[frame_id];
    if (old_page.IsDirty()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();
      disk_scheduler_->Schedule({true, old_page.GetData(), old_page.GetPageId(),std::move(promise)});
      future.get();
    }
    page_table_.erase(old_page.GetPageId());
  }
  
  Page &page = pages_[frame_id];
  page.ResetMemory();
  page.page_id_ = page_id;
  page.is_dirty_ = false;
  page.pin_count_ = 1;
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({false, page.GetData(), page_id,std::move(promise)});
  future.get();
  page_table_[page_id] = frame_id;
  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);
  
  return &page;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }
  
  Page &page = pages_[it->second];
  if (page.pin_count_ <= 0) {
    return false;
  }
  
  if (is_dirty) {
    page.is_dirty_ = true;} 
    page.pin_count_--;
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(it->second, true);
  }
  
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }  
  Page &page = pages_[it->second];
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, page.GetData(), page_id, std::move(promise)});
  future.get();
  page.is_dirty_ = false;
  
  return true;
 }

void BufferPoolManager::FlushAllPages() {
  std::scoped_lock<std::mutex> lock(latch_);
  
  for (auto &[pid, fid] : page_table_) {
    Page &page = pages_[fid];
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, page.GetData(), pid, std::move(promise)});
    future.get();
    page.is_dirty_ = false;
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
  std::scoped_lock<std::mutex> lock(latch_);
  
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;
  }
  
  Page &page = pages_[it->second];
  if (page.pin_count_ > 0) {
    return false;
  }
  
  frame_id_t frame_id = it->second;
  page_table_.erase(it);
  replacer_->Remove(frame_id);
  
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.is_dirty_ = false;
  page.pin_count_ = 0;
  
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  
  return true;
 }

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub

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

BufferPoolManager::~BufferPoolManager() { 
   std::cout << "Hit: " << hit_count_ << ", Miss: " << miss_count_ 
              << ", Rate: " << (double)hit_count_ / (hit_count_ + miss_count_) <<",pool_size"<<pool_size_<< std::endl;
    delete[] pages_;
 }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id;
  Page *victim = nullptr;
  bool need_flush = false;
  page_id_t old_pid = INVALID_PAGE_ID;
  char temp_buffer[BUSTUB_PAGE_SIZE];
  // === 第一段：持锁找 frame ===
  {
    std::scoped_lock<std::mutex> lock(latch_);
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else {
      if (!replacer_->Evict(&frame_id)) {
        return nullptr;
      }
      Page &old_page = pages_[frame_id];
      victim = &pages_[frame_id];
      old_pid = victim->GetPageId();
      need_flush = victim->IsDirty();
      std::memcpy(temp_buffer, old_page.data_, BUSTUB_PAGE_SIZE);
      page_table_.erase(old_pid);
      replacer_->Remove(frame_id);
    }
    *page_id = AllocatePage();
    Page &page = pages_[frame_id];
    page.is_loading = true;
    page.page_id_ = *page_id;
    page.pin_count_ = 1;
    page.is_dirty_ = false;
    page_table_[*page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }  // 释放锁

  // === 第二段：无锁刷盘 ===
  if (need_flush) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true,temp_buffer, old_pid, std::move(promise)});
    future.get();
  }

  // === 第三段：持锁初始化新页 ===
  {
    std::scoped_lock<std::mutex> lock(latch_);
    Page &page = pages_[frame_id];
    page.ResetMemory();
    page.is_loading = false;
    page.cv_.notify_all();
    return &page;
  }
}
auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType) -> Page * {
  frame_id_t frame_id=-1;
  bool need_flush = false;
  bool cache_hit = false;
  page_id_t old_pid = INVALID_PAGE_ID;
  char temp_buffer[BUSTUB_PAGE_SIZE];
  std::optional<std::future<bool>> flush_future;

  // === 阶段1：持锁找 frame ===
  {
    std::unique_lock<std::mutex> lock(latch_);
  while (true) {
  auto it = page_table_.find(page_id);  
  if (it == page_table_.end()) break;

  Page &page = pages_[it->second];
  if (page.is_loading) {
    page.cv_.wait(lock);
    continue;
  }
  page.pin_count_++;
  replacer_->RecordAccess(it->second);
  replacer_->SetEvictable(it->second, false);
  return &page;
}
    // 找空闲 frame
    if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    pages_[frame_id].is_loading = true;
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ = 1;
    pages_[frame_id].is_dirty_ = false;
    page_table_[page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    } else {
      if (!replacer_->Evict(&frame_id)) {
        return nullptr;
      }
    Page &old_page = pages_[frame_id];
  old_pid = old_page.GetPageId();
  need_flush = old_page.IsDirty();
  page_table_.erase(old_pid);
  std::memcpy(temp_buffer, old_page.data_, BUSTUB_PAGE_SIZE);
  pages_[frame_id].is_loading = true;
  pages_[frame_id].page_id_ = page_id;         // 可以提前写，也可以等阶段4再写
  pages_[frame_id].pin_count_ = 1;             // 先 pin 住，防止被再次 evict
  pages_[frame_id].is_dirty_ = false;
  page_table_[page_id] = frame_id;             // 提前插入映射！！
  replacer_->RecordAccess(frame_id);           // 可以提前记录
  replacer_->SetEvictable(frame_id, false);  
    }
  }

  // === 阶段2：无锁刷脏页 ===
  if (need_flush) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true,temp_buffer, old_pid, std::move(promise)});
    future.get();
  }
  // === 阶段3：无锁读新页 ===
  
  {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({false, pages_[frame_id].GetData(), page_id, std::move(promise)});
    future.get();
  }

  // === 阶段4：持锁更新元数据 ===
  {
  std::scoped_lock<std::mutex> lock(latch_);
  Page &page = pages_[frame_id];
  page.is_loading = false;           
  page.cv_.notify_all();
  return &page;
  }  
}


auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  char* data_copy = nullptr;
  page_id_t pid;
  
  // 阶段1：持锁拷贝数据
  {
    std::scoped_lock<std::mutex> lock(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      return false;
    }
    Page &page = pages_[it->second];
    pid = page.GetPageId();
    data_copy = page.GetData();
    page.is_dirty_ = false;
  }
  
  // 阶段2：无锁刷盘
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, data_copy, pid, std::move(promise)});
  future.get();
  
  return true;
}



void BufferPoolManager::FlushAllPages() {
  std::vector<std::pair<page_id_t, char*>> to_flush;
  std::vector<std::future<bool>> futures;
  
  // 阶段1：持锁收集所有需要刷的页
  {
    std::scoped_lock<std::mutex> lock(latch_);
    for (auto &[pid, fid] : page_table_) {
      to_flush.emplace_back(pid, pages_[fid].GetData());
      pages_[fid].is_dirty_ = false;
    }
  }
  
  // 阶段2：无锁并行提交所有刷盘请求
  for (auto &[pid, data] : to_flush) {
    auto promise = disk_scheduler_->CreatePromise();
    futures.push_back(promise.get_future());
    disk_scheduler_->Schedule({true, data, pid, std::move(promise)});
  }
  
  // 阶段3：等待所有完成
  for (auto &f : futures) {
    f.get();
  }
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
        return false;  
    }
    frame_id_t frame_id = it->second;
    Page &page = pages_[frame_id];
    int current = page.pin_count_.load(std::memory_order_relaxed);
    if (current <= 0) {
        return false;
    }
    if (is_dirty) {
        page.is_dirty_ = true;
    }
    page.pin_count_--;
    if (page.pin_count_== 0) {
        replacer_->SetEvictable(frame_id, true);
    }
    return true;
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

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
#include <array>
#include "buffer/buffer_pool_manager.h"
#include "storage/page/page.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size),
      disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)),
      log_manager_(log_manager),
      frame_latches_(pool_size) {
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { 
    delete[] pages_;
 }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  frame_id_t frame_id = INVALID_FRAME_ID;
  bool need_flush = false;
  page_id_t old_pid = INVALID_PAGE_ID;
  std::array<char, BUSTUB_PAGE_SIZE> flush_buffer{};
  {
    std::unique_lock<std::mutex> dir_lock(latch_);
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (!replacer_->Evict(&frame_id)) {
      return nullptr;
    }

    std::unique_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    Page &frame = pages_[frame_id];
    if (frame.GetPageId() != INVALID_PAGE_ID) {
      old_pid = frame.GetPageId();
      need_flush = frame.IsDirty();
      if (need_flush) {
        std::memcpy(flush_buffer.data(), frame.GetData(), BUSTUB_PAGE_SIZE);
        in_flight_flush_.insert(old_pid);
      }
      page_table_.erase(old_pid);
    }

    *page_id = AllocatePage();
    frame.page_id_ = *page_id;
    frame.pin_count_ = 1;
    frame.is_dirty_ = false;
    frame.is_loading = true;
    page_table_[*page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }

  if (need_flush) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, flush_buffer.data(), old_pid, std::move(promise)});
    future.get();
  }

  {
    std::unique_lock<std::mutex> dir_lock(latch_);
    std::unique_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    if (need_flush) {
      in_flight_flush_.erase(old_pid);
      flush_inflight_cv_.notify_all();
    }
    Page &frame = pages_[frame_id];
    frame.ResetMemory();
    frame.is_loading = false;
    frame.cv_.notify_all();
    return &frame;
  }
}

auto BufferPoolManager::FetchPage(page_id_t page_id, AccessType) -> Page * {
  frame_id_t frame_id = INVALID_FRAME_ID;
  bool need_flush = false;
  page_id_t old_pid = INVALID_PAGE_ID;
  std::array<char, BUSTUB_PAGE_SIZE> flush_buffer{};
  std::shared_ptr<std::condition_variable> pending_cv;

  {
    std::unique_lock<std::mutex> dir_lock(latch_);
    while (true) {
      auto page_it = page_table_.find(page_id);
      if (page_it != page_table_.end()) {
        frame_id_t found_frame = page_it->second;
        std::unique_lock<std::mutex> frame_lock(frame_latches_[found_frame]);
        Page &found_page = pages_[found_frame];
        if (found_page.is_loading) {
          frame_lock.unlock();
          found_page.cv_.wait(dir_lock);
          continue;
        }
        found_page.pin_count_++;
        replacer_->RecordAccess(found_frame);
        replacer_->SetEvictable(found_frame, false);
        return &found_page;
      }

      while (in_flight_flush_.count(page_id) != 0U) {
        flush_inflight_cv_.wait(dir_lock);
      }

      auto pending_it = pending_fetches_.find(page_id);
      if (pending_it != pending_fetches_.end()) {
        pending_it->second->wait(dir_lock);
        continue;
      }
      pending_cv = std::make_shared<std::condition_variable>();
      pending_fetches_[page_id] = pending_cv;
      break;
    }

    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (!replacer_->Evict(&frame_id)) {
      pending_fetches_.erase(page_id);
      pending_cv->notify_all();
      return nullptr;
    }

    std::unique_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    Page &frame = pages_[frame_id];
    if (frame.GetPageId() != INVALID_PAGE_ID) {
      old_pid = frame.GetPageId();
      need_flush = frame.IsDirty();
      if (need_flush) {
        std::memcpy(flush_buffer.data(), frame.GetData(), BUSTUB_PAGE_SIZE);
        in_flight_flush_.insert(old_pid);
      }
      page_table_.erase(old_pid);
    }
    frame.is_loading = true;
    frame.page_id_ = INVALID_PAGE_ID;
    frame.pin_count_ = 1;
    frame.is_dirty_ = false;
    page_table_[page_id] = frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
  }

  if (need_flush) {
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, flush_buffer.data(), old_pid, std::move(promise)});
    future.get();
    std::unique_lock<std::mutex> dir_lock(latch_);
    in_flight_flush_.erase(old_pid);
    flush_inflight_cv_.notify_all();
  }

  auto read_promise = disk_scheduler_->CreatePromise();
  auto read_future = read_promise.get_future();
  disk_scheduler_->Schedule({false, pages_[frame_id].GetData(), page_id, std::move(read_promise)});
  read_future.get();

  {
    std::unique_lock<std::mutex> dir_lock(latch_);
    std::unique_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    Page &frame = pages_[frame_id];
    frame.page_id_ = page_id;
    frame.is_loading = false;
    frame.cv_.notify_all();
    pending_fetches_.erase(page_id);
    pending_cv->notify_all();
    return &frame;
  }
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::array<char, BUSTUB_PAGE_SIZE> temp_buffer{};
  page_id_t pid = INVALID_PAGE_ID;
  frame_id_t frame_id = INVALID_FRAME_ID;

  {
    std::unique_lock<std::mutex> dir_lock(latch_);
    auto page_it = page_table_.find(page_id);
    if (page_it == page_table_.end()) {
      return false;
    }
    frame_id = page_it->second;
    std::unique_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
    Page &page = pages_[frame_id];
    pid = page.GetPageId();
    std::memcpy(temp_buffer.data(), page.GetData(), BUSTUB_PAGE_SIZE);
    page.is_dirty_ = false;
  }

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  disk_scheduler_->Schedule({true, temp_buffer.data(), pid, std::move(promise)});
  future.get();

  return true;
}
void BufferPoolManager::FlushAllPages() {
  std::vector<std::pair<page_id_t, frame_id_t>> snapshot;
  std::vector<std::pair<page_id_t, std::array<char, BUSTUB_PAGE_SIZE>>> to_flush;
  std::vector<std::future<bool>> futures;

  {
    std::unique_lock<std::mutex> dir_lock(latch_);
    snapshot.reserve(page_table_.size());
    for (const auto &[pid, fid] : page_table_) {
      snapshot.emplace_back(pid, fid);
    }
  }

  to_flush.reserve(snapshot.size());
  for (const auto &[pid, fid] : snapshot) {
    std::unique_lock<std::mutex> frame_lock(frame_latches_[fid]);
    Page &page = pages_[fid];
    if (page.GetPageId() != pid) {
      continue;
    }
    std::array<char, BUSTUB_PAGE_SIZE> buf{};
    std::memcpy(buf.data(), page.GetData(), BUSTUB_PAGE_SIZE);
    page.is_dirty_ = false;
    to_flush.emplace_back(pid, std::move(buf));
  }

  for (auto &[pid, data] : to_flush) {
    auto promise = disk_scheduler_->CreatePromise();
    futures.push_back(promise.get_future());
    disk_scheduler_->Schedule({true, data.data(), pid, std::move(promise)});
  }
  
  for (auto &f : futures) {
    f.get();
  }
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, AccessType access_type) -> bool {
  static_cast<void>(access_type);
  std::unique_lock<std::mutex> dir_lock(latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = page_it->second;
  std::unique_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
  Page &page = pages_[frame_id];

  int pin_count = page.pin_count_.load(std::memory_order_relaxed);
  if (pin_count <= 0) {
    return false;
  }

  if (is_dirty) {
    page.is_dirty_ = true;
  }

  page.pin_count_--;
  BUSTUB_ASSERT(page.pin_count_.load(std::memory_order_relaxed) >= 0, "pin_count should not be negative");
  if (page.pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> dir_lock(latch_);
  auto page_it = page_table_.find(page_id);
  if (page_it == page_table_.end()) {
    return true;
  }

  frame_id_t frame_id = page_it->second;
  std::unique_lock<std::mutex> frame_lock(frame_latches_[frame_id]);
  Page &page = pages_[frame_id];
  if (page.pin_count_ > 0) {
    return false;
  }

  page_table_.erase(page_it);
  replacer_->Remove(frame_id);
  page.ResetMemory();
  page.page_id_ = INVALID_PAGE_ID;
  page.is_loading = false;
  page.is_dirty_ = false;
  page.pin_count_ = 0;
  free_list_.push_back(frame_id);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
  Page *page=FetchPage(page_id);
  if (page != nullptr) {
        return {this, page};
  }
  return {this, nullptr};
 }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { 
  Page *page=FetchPage(page_id);
  if (page != nullptr) {
    page->RLatch();
    return {this, page};
  }
  return {this, nullptr}; 
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
  Page *page=FetchPage(page_id);
  if (page != nullptr) {
    page->WLatch();
    return {this, page};
  }
  return {this, nullptr}; 
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { 
  Page *page=NewPage(page_id);
  if(page!=nullptr){
    return {this, page};
  }
  return {this, nullptr}; 
}

}  // namespace bustub
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager, size_t num_workers)
    : disk_manager_(disk_manager) {
  // 启动 num_workers 个线程
  for (size_t i = 0; i < num_workers; i++) {
    workers_.emplace_back([this] { StartWorkerThread(); });
  }
}


DiskScheduler::~DiskScheduler() {
  // 发送 num_workers 个终止信号
  for (size_t i = 0; i < workers_.size(); i++) {
    request_queue_.Put(std::nullopt);
  }
  // 等待所有线程结束
  for (auto &t : workers_) {
    t.join();
  }
}

void DiskScheduler::Schedule(DiskRequest r) {
  request_queue_.Put(std::move(r));
}

void DiskScheduler::StartWorkerThread() {
  while (true) {
        auto request = request_queue_.Get();      
        if (!request.has_value()) break;
        if (request->is_write_) {
            disk_manager_->WritePage(request->page_id_, request->data_);
        } else {
            disk_manager_->ReadPage(request->page_id_, request->data_);
        }
        request->callback_.set_value(true);
    }
}

}  // namespace bustub

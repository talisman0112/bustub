#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    this->bpm_ = that.bpm_;
    this->page_ = that.page_;
    this->is_dirty_ = that.is_dirty_;
    that.bpm_ = nullptr;
    that.page_ = nullptr;
    that.is_dirty_ = false;
}

void BasicPageGuard::Drop() {
    if (bpm_ == nullptr || page_ == nullptr) {
    return;
  }
  bpm_->UnpinPage(page_->GetPageId(), is_dirty_);
  bpm_ = nullptr;
  page_ = nullptr;
  is_dirty_ = false;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    if (this == &that) {
    return *this;
  }
  Drop();
  this->bpm_ = that.bpm_;
  this->page_ = that.page_;
  this->is_dirty_ = that.is_dirty_;
  that.bpm_ = nullptr;
  that.page_ = nullptr;
  that.is_dirty_ = false;
  return *this;
 }

 auto BasicPageGuard::UpgradeWrite() -> WritePageGuard {
  if (page_ == nullptr) {
    return {nullptr, nullptr};
  }
  page_->WLatch();
  auto guard = WritePageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return guard;
}
auto BasicPageGuard::UpgradeRead() -> ReadPageGuard {
  if (page_ == nullptr) {
    return {nullptr, nullptr};
  }
  page_->WLatch();
  auto guard = ReadPageGuard(bpm_, page_);
  bpm_ = nullptr;
  page_ = nullptr;
  return guard;
}
BasicPageGuard::~BasicPageGuard(){
    Drop();
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    if(this==&that){
        return *this;
    }
    this->Drop();
    this->guard_ = std::move(that.guard_);
    return *this;
 }

void ReadPageGuard::Drop() {
    if(guard_.page_!=nullptr){
        guard_.page_->RUnlatch();
    }
    guard_.Drop();
}

ReadPageGuard::~ReadPageGuard() {
     Drop();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
    if(this==&that){
        return *this;
    }
    this->Drop();
    this->guard_ = std::move(that.guard_);
    return *this;
 }

void WritePageGuard::Drop() {
    if(guard_.page_!=nullptr){
        guard_.page_->WUnlatch();
    }
    guard_.Drop();
}

WritePageGuard::~WritePageGuard() {
    Drop();
}  // NOLINT
    
}  // namespace bustub

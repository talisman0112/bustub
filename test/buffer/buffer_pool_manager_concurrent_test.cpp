#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

TEST(BufferPoolManagerConcurrentTest, PendingFetchSingleLoader) {
  const std::string db_file = "bpm_pending_single_loader.db";
  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto bpm = std::make_unique<BufferPoolManager>(1, disk_manager.get(), 2);

  page_id_t page0 = INVALID_PAGE_ID;
  page_id_t page1 = INVALID_PAGE_ID;
  auto *first = bpm->NewPage(&page0);
  ASSERT_NE(first, nullptr);
  first->GetData()[0] = 'X';
  ASSERT_TRUE(bpm->UnpinPage(page0, true));

  auto *second = bpm->NewPage(&page1);
  ASSERT_NE(second, nullptr);
  second->GetData()[0] = 'Y';
  ASSERT_TRUE(bpm->UnpinPage(page1, true));

  constexpr size_t kThreads = 16;
  std::vector<std::thread> threads;
  std::vector<Page *> fetched(kThreads, nullptr);
  std::atomic<bool> failed{false};
  for (size_t i = 0; i < kThreads; i++) {
    threads.emplace_back([&, i]() {
      auto *page = bpm->FetchPage(page0);
      if (page == nullptr || page->GetData()[0] != 'X') {
        failed.store(true);
        return;
      }
      fetched[i] = page;
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  for (size_t i = 1; i < kThreads; i++) {
    EXPECT_EQ(fetched[i], fetched[0]);
  }
  EXPECT_FALSE(failed.load());
  ASSERT_NE(fetched[0], nullptr);
  EXPECT_EQ(fetched[0]->GetPinCount(), static_cast<int>(kThreads));

  for (size_t i = 0; i < kThreads; i++) {
    ASSERT_TRUE(bpm->UnpinPage(page0, false));
  }

  bpm->FlushAllPages();
  bpm.reset();
  disk_manager->ShutDown();
  disk_manager.reset();
  std::remove(db_file.c_str());
}

TEST(BufferPoolManagerConcurrentTest, EvictUnderContention) {
  const std::string db_file = "bpm_evict_contention.db";
  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_manager.get(), 2);

  constexpr size_t kPageCount = 12;
  std::vector<page_id_t> page_ids;
  page_ids.reserve(kPageCount);
  std::vector<char> expected(kPageCount);

  for (size_t i = 0; i < kPageCount; i++) {
    page_id_t page_id = INVALID_PAGE_ID;
    auto *page = bpm->NewPage(&page_id);
    ASSERT_NE(page, nullptr);
    expected[i] = static_cast<char>(i + 1);
    page->GetData()[0] = expected[i];
    ASSERT_TRUE(bpm->UnpinPage(page_id, true));
    page_ids.push_back(page_id);
  }

  constexpr size_t kThreads = 8;
  constexpr size_t kIters = 1200;
  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (size_t thread_id = 0; thread_id < kThreads; thread_id++) {
    threads.emplace_back([&, thread_id]() {
      std::mt19937 gen(static_cast<uint32_t>(thread_id + 7));
      std::uniform_int_distribution<size_t> dist(0, kPageCount - 1);
      for (size_t iter = 0; iter < kIters; iter++) {
        size_t idx = dist(gen);
        auto *page = bpm->FetchPage(page_ids[idx]);
        if (page == nullptr) {
          failed.store(true);
          return;
        }
        if (page->GetData()[0] != expected[idx]) {
          failed.store(true);
        }
        if (!bpm->UnpinPage(page_ids[idx], false)) {
          failed.store(true);
        }
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  EXPECT_FALSE(failed.load());

  bpm->FlushAllPages();
  bpm.reset();
  disk_manager->ShutDown();
  disk_manager.reset();
  std::remove(db_file.c_str());
}

TEST(BufferPoolManagerConcurrentTest, OppositeAccessPatternsNoDeadlock) {
  using namespace std::chrono_literals;

  const std::string db_file = "bpm_deadlock_regression.db";
  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto bpm = std::make_unique<BufferPoolManager>(4, disk_manager.get(), 2);

  page_id_t p0 = INVALID_PAGE_ID;
  page_id_t p1 = INVALID_PAGE_ID;
  ASSERT_NE(bpm->NewPage(&p0), nullptr);
  ASSERT_NE(bpm->NewPage(&p1), nullptr);
  ASSERT_TRUE(bpm->UnpinPage(p0, false));
  ASSERT_TRUE(bpm->UnpinPage(p1, false));

  auto worker = [&](page_id_t first, page_id_t second) {
    for (size_t i = 0; i < 2500; i++) {
      auto *page_a = bpm->FetchPage(first);
      if (page_a == nullptr) {
        return false;
      }
      auto *page_b = bpm->FetchPage(second);
      if (page_b == nullptr) {
        return false;
      }
      if (!bpm->UnpinPage(second, false)) {
        return false;
      }
      if (!bpm->UnpinPage(first, false)) {
        return false;
      }
    }
    return true;
  };

  auto fut_a = std::async(std::launch::async, worker, p0, p1);
  auto fut_b = std::async(std::launch::async, worker, p1, p0);

  EXPECT_EQ(fut_a.wait_for(10s), std::future_status::ready);
  EXPECT_EQ(fut_b.wait_for(10s), std::future_status::ready);
  EXPECT_TRUE(fut_a.get());
  EXPECT_TRUE(fut_b.get());

  bpm->FlushAllPages();
  bpm.reset();
  disk_manager->ShutDown();
  disk_manager.reset();
  std::remove(db_file.c_str());
}

}  // namespace bustub

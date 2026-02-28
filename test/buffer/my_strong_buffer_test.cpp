#include <thread>
#include <vector>
#include <cstdlib>
#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_k_replacer.h"
#include "storage/disk/disk_manager.h"
#include "gtest/gtest.h"

namespace bustub {

// ==================== LRU-K 强测 ====================
TEST(MyStrongTest, LRUK_KthTimestampAndEvictOrder) {
  LRUKReplacer replacer(10, 2);
  frame_id_t victim;

  replacer.RecordAccess(1);
  replacer.SetEvictable(1, true);

  replacer.RecordAccess(2);
  replacer.RecordAccess(2);
  replacer.RecordAccess(2);
  replacer.SetEvictable(2, true);

  replacer.RecordAccess(3);
  replacer.SetEvictable(3, true);

  EXPECT_EQ(replacer.Size(), 3);

  // 第一次：淘汰 less_k 中最老的 frame 1
  EXPECT_TRUE(replacer.Evict(&victim));
  EXPECT_EQ(victim, 1);

  replacer.RecordAccess(2);

  // 第二次：less_k 还有 frame 3，必须先淘汰它
  EXPECT_TRUE(replacer.Evict(&victim));
  EXPECT_EQ(victim, 3);  // ← 修正！

  // 第三次：只剩 frame 2
  EXPECT_TRUE(replacer.Evict(&victim));
  EXPECT_EQ(victim, 2);
}


TEST(MyStrongTest, LRUK_RemoveAndSetEvictable) {
  LRUKReplacer replacer(5, 2);
  replacer.RecordAccess(1);
  replacer.RecordAccess(1);
  replacer.SetEvictable(1, true);

  EXPECT_EQ(replacer.Size(), 1);

  replacer.Remove(1);                    // 必须能删除
  EXPECT_EQ(replacer.Size(), 0);

  // 删除后不能再 evict
  frame_id_t victim;
  EXPECT_FALSE(replacer.Evict(&victim));
}

// ==================== BufferPoolManager 强测 ====================
TEST(MyStrongTest, ConcurrentFetchSamePage) {
  const std::string db_file = "my_strong_test.db";
  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto bpm = std::make_unique<BufferPoolManager>(10, disk_manager.get(), 2);

  // ★★★ 先创建页面 ★★★
  page_id_t pid;
  auto *new_page = bpm->NewPage(&pid);
  ASSERT_NE(new_page, nullptr);
  
  // 写点数据进去（可选）
  snprintf(new_page->GetData(), BUSTUB_PAGE_SIZE, "Hello BusTub!");
  
  // Unpin 让它可以被 evict（但这里 buffer pool 够大，不会被 evict）
  bpm->UnpinPage(pid, true);

  // 现在并发 Fetch 这个已存在的页面
  std::vector<std::thread> threads;
  std::vector<Page*> pages(30, nullptr);
  std::mutex m;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 30; ++i) {
    threads.emplace_back([&, i]() {  // 注意捕获 i by value
      auto *page = bpm->FetchPage(pid);
      if (page != nullptr) {
        pages[i] = page;
        success_count++;
      }
    });
  }
  for (auto &t : threads) t.join();

  EXPECT_EQ(success_count.load(), 30);
  
  // 所有线程必须拿到同一个 Page 对象
  for (int i = 0; i < 30; ++i) {
    EXPECT_EQ(pages[i], pages[0]);
  }
  
  // pin_count 应该是 30（每个 FetchPage 都 +1）
  EXPECT_EQ(pages[0]->GetPinCount(), 30);

  // 清理：Unpin 30 次
  for (int i = 0; i < 30; ++i) {
    bpm->UnpinPage(pid, false);
  }

  bpm->FlushAllPages();
  bpm.reset();
  disk_manager.reset();
  std::remove(db_file.c_str());
}


TEST(MyStrongTest, EvictDirtyPageAndFlush) {
  const std::string db_file = "my_strong_test2.db";
  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto bpm = std::make_unique<BufferPoolManager>(3, disk_manager.get(), 2);  // 小池子容易触发 evict

  page_id_t p1, p2, p3;
  Page *page1 = bpm->NewPage(&p1);
  Page *page2 = bpm->NewPage(&p2);
  Page *page3 = bpm->NewPage(&p3);

  // 填满池子
  EXPECT_NE(page1, nullptr);
  EXPECT_NE(page2, nullptr);
  EXPECT_NE(page3, nullptr);

  // 标记脏页
  page1->GetData()[0] = 'X';
  page2->GetData()[0] = 'Y';

  bpm->UnpinPage(p1, true);
  bpm->UnpinPage(p2, true);
  bpm->UnpinPage(p3, false);

  // 再 New 一个，应该触发 evict 脏页
  page_id_t p4;
  Page *page4 = bpm->NewPage(&p4);
  EXPECT_NE(page4, nullptr);

  bpm->FlushAllPages();

  bpm.reset();
  disk_manager.reset();
  std::remove(db_file.c_str());
}

TEST(MyStrongTest, DeletePinnedPageShouldFail) {
  const std::string db_file = "my_strong_test3.db";
  auto disk_manager = std::make_unique<DiskManager>(db_file);
  auto bpm = std::make_unique<BufferPoolManager>(5, disk_manager.get(), 2);

  page_id_t pid;
  auto *page = bpm->NewPage(&pid);
  EXPECT_NE(page, nullptr);

  // 未 unpin 就 Delete，应该失败
  EXPECT_FALSE(bpm->DeletePage(pid));

  bpm->UnpinPage(pid, false);
  EXPECT_TRUE(bpm->DeletePage(pid));

  bpm.reset();
  disk_manager.reset();
  std::remove(db_file.c_str());
}

}  // namespace bustub
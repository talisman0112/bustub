//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <vector>
#include <unordered_map>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "type/value.h"
#include "common/util/hash_util.h"

namespace bustub {

// ✅ 定义 CompositeKey
struct CompositeKey {
  std::vector<Value> keys_;
  
  auto operator==(const CompositeKey &other) const -> bool {
    if (keys_.size() != other.keys_.size()) return false;
    for (size_t i = 0; i < keys_.size(); i++) {
      if (keys_[i].CompareEquals(other.keys_[i]) != CmpBool::CmpTrue) {
        return false;
      }
    }
    return true;
  }
};

//  定义 Hasher
struct CompositeKeyHasher {
  auto operator()(const CompositeKey &key) const -> std::size_t {
    std::size_t hash = 0;
    for (const auto &val : key.keys_) {
      hash = HashUtil::CombineHashes(hash, HashUtil::HashValue(&val));
    }
    return hash;
  }
};

class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, 
                   std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;
  auto Next(Tuple *tuple, RID *rid) -> bool override;
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

 private:
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_executor_;
  std::unique_ptr<AbstractExecutor> right_executor_;
  // 用 CompositeKey 替换 AggregateKey
  std::unordered_map<CompositeKey, std::vector<Tuple>, CompositeKeyHasher> hash_table_;
  std::vector<Tuple> current_matches_;
  Tuple left_tuple_;
};

}  // namespace bustub

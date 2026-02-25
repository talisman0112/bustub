//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::scoped_lock<std::mutex> lock(latch_);
   if (!less_k_list.empty()) {
        frame_id_t victim = less_k_list.back();
        less_k_list.pop_back();
        less_k_map.erase(victim);
        node_store_.erase(victim);
        curr_size_--;
        *frame_id = victim;
        return true;
    }
    else if (!more_k_set.empty()) {
        auto it = more_k_set.begin();
        frame_id_t victim = it->second;
        more_k_set.erase(it);
        more_k_map.erase(victim);
        node_store_.erase(victim);
        curr_size_--;
        *frame_id = victim;
        return true;
    }
    return false;
 }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::scoped_lock<std::mutex> lock(latch_);

    if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
        throw std::exception();
    }
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        LRUKNode node;
        node.k_ = k_;
        node.fid_ = frame_id;
        node.history_ = {};
        node.is_evictable_ = false;
        node_store_.insert({frame_id, node});
        it = node_store_.find(frame_id);  // 重新获取it
    }
    LRUKNode &node = it->second;
    node.history_.push_back(current_timestamp_);
    current_timestamp_++;
    while (node.history_.size() > k_) {
        node.history_.pop_front();
    }
    if (!node.is_evictable_) {
        return;
    }
    size_t hist_size = node.history_.size();
    if (hist_size < k_) {
        // 存在，弄到头部
        auto map_it = less_k_map.find(frame_id);
        if (map_it != less_k_map.end()) {
            less_k_list.splice(less_k_list.begin(), less_k_list, map_it->second);
            less_k_map[frame_id] = less_k_list.begin();
        } else {
            // 新进入less_k_list
            less_k_list.push_front(frame_id);
            less_k_map[frame_id] = less_k_list.begin();
        }
    } else {
        // >= k
        // 检查是否刚跨越k（从k-1到k）
        if (hist_size == k_) {
            // 从less_k移除（如果存在）
            auto map_it = less_k_map.find(frame_id);
            if (map_it != less_k_map.end()) {
                less_k_list.erase(map_it->second);
                less_k_map.erase(frame_id);
            }
        }
        // 更新more_k_set
        auto set_it = more_k_map.find(frame_id);
        if (set_it != more_k_map.end()) {
            // 先erase旧的
            size_t old_kth = set_it->second;
            more_k_set.erase({old_kth, frame_id});
        }
        // 保持history size == k（满了pop_front）
        if (node.history_.size() > k_) {
            node.history_.pop_front();
        }
        // 新kth_ts = front()（最老的）
        size_t new_kth_ts = node.history_.front();
        // 插入新的
        more_k_set.insert({new_kth_ts, frame_id});
        more_k_map[frame_id] = new_kth_ts;
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        return;
    }
    LRUKNode &node = it->second;
    if (node.is_evictable_ == set_evictable) {
        return;
    }
    if (set_evictable) {
        // false到true，加入替换
        node.is_evictable_ = true;
        curr_size_++;
        size_t hist_size = node.history_.size();
        if (hist_size < k_) {
            less_k_list.push_front(frame_id);
            less_k_map[frame_id] = less_k_list.begin();
        } else if (!node.history_.empty()) {  // 防止 history 为空
            size_t kth_ts = node.history_.front();
            more_k_set.insert({kth_ts, frame_id});
            more_k_map[frame_id] = kth_ts;
        }
    } else {
        // true到false，移出替换
        node.is_evictable_ = false;
        curr_size_--;
        // 从 less_k 移除
        auto lit = less_k_map.find(frame_id);
        if (lit != less_k_map.end()) {
            less_k_list.erase(lit->second);
            less_k_map.erase(frame_id);
        }
        auto mit = more_k_map.find(frame_id);
        if (mit != more_k_map.end()) {
            size_t old_kth = mit->second;
            more_k_set.erase({old_kth, frame_id});
            more_k_map.erase(frame_id);
        }
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        return;
    }
    if (!it->second.is_evictable_) {
        throw std::exception();
    }
    // 清理 less_k
    auto lit = less_k_map.find(frame_id);
    if (lit != less_k_map.end()) {
        less_k_list.erase(lit->second);
        less_k_map.erase(lit);
    }
    // 清理 more_k
    auto mit = more_k_map.find(frame_id);
    if (mit != more_k_map.end()) {
        more_k_set.erase({mit->second, frame_id});
        more_k_map.erase(mit);
    }
    node_store_.erase(it);
    curr_size_--;
}


auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub


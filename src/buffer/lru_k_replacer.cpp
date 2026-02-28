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
        it = node_store_.find(frame_id);
    }
    LRUKNode &node = it->second;
    node.history_.push_back(current_timestamp_);
    current_timestamp_++;

    if (!node.is_evictable_) {
        return;
    }

    // 先修剪 history，永远保持 <= k 个
    while (node.history_.size() > k_) {
        node.history_.pop_front();
    }

    size_t sz = node.history_.size();  

    if (sz < k_) {
        // < k 次，维护 less_k 的 LRU 顺序
        auto map_it = less_k_map.find(frame_id);
        if (map_it != less_k_map.end()) {
            less_k_list.splice(less_k_list.begin(), less_k_list, map_it->second);
            less_k_map[frame_id] = less_k_list.begin();
        } else {
            less_k_list.push_front(frame_id);
            less_k_map[frame_id] = less_k_list.begin();
        }
    } else {
        // == k 次（因为 > k 已经被 pop 掉了）
        //  more_k
        auto lit = less_k_map.find(frame_id);
        if (lit != less_k_map.end()) {
            less_k_list.erase(lit->second);
            less_k_map.erase(frame_id);
        }
        size_t kth_ts = node.history_.front();  
        auto moreit = more_k_map.find(frame_id);
        if (moreit != more_k_map.end()) {
            more_k_set.erase({moreit->second, frame_id});
            more_k_map.erase(frame_id);
        }

        more_k_set.insert({kth_ts, frame_id});
        more_k_map[frame_id] = kth_ts;
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
        node.is_evictable_ = true;
        curr_size_++;
        // 同步修历史
        while (node.history_.size() > k_) {
            node.history_.pop_front();
        }
        size_t sz = node.history_.size();

        if (sz < k_) {
            less_k_list.push_front(frame_id);
            less_k_map[frame_id] = less_k_list.begin();
        } else if (sz >= k_ && !node.history_.empty()) {
            size_t kth_ts = node.history_.front();
            auto mit = more_k_map.find(frame_id);
            if (mit != more_k_map.end()) {
                more_k_set.erase({mit->second, frame_id});
                more_k_map.erase(frame_id);
            }

            more_k_set.insert({kth_ts, frame_id});
            more_k_map[frame_id] = kth_ts;
        }
    } else {
        node.is_evictable_ = false;
        curr_size_--;

        auto lit = less_k_map.find(frame_id);
        if (lit != less_k_map.end()) {
            less_k_list.erase(lit->second);
            less_k_map.erase(frame_id);
        }
        auto mit = more_k_map.find(frame_id);
        if (mit != more_k_map.end()) {
            more_k_set.erase({mit->second, frame_id});
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
    // 清less_k
    auto lit = less_k_map.find(frame_id);
    if (lit != less_k_map.end()) {
        less_k_list.erase(lit->second);
        less_k_map.erase(lit);
    }
    // 清 more_k
    auto mit = more_k_map.find(frame_id);
    if (mit != more_k_map.end()) {
        more_k_set.erase({mit->second, frame_id});
        more_k_map.erase(mit);
    }
    node_store_.erase(it);
    curr_size_--;
}


auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustubp


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
    // 优先淘汰 less_k（按 last_timestamp 升序，最小的先淘汰）
    if (!less_k_set.empty()) {
        auto it = less_k_set.begin();
        frame_id_t victim = it->second;      
        less_k_set.erase(it);
        less_k_map.erase(victim);
        node_store_.erase(victim);
        curr_size_--;
        *frame_id = victim;
        return true;
    }
    
    // 再淘汰 more_k
    if (!more_k_set.empty()) {
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
    size_t new_ts = current_timestamp_++;
    node.history_.push_back(new_ts);
    //history，永远 <= k
    while (node.history_.size() > k_) {
        node.history_.pop_front();
    }

    if (!node.is_evictable_) {
        return;
    }

    size_t sz = node.history_.size();

    if (sz < k_) {
        // < k：更新 less_k_set 中的位置
        auto map_it = less_k_map.find(frame_id);
        if (map_it != less_k_map.end()) {
            less_k_set.erase({map_it->second, frame_id});
        }
        less_k_set.insert({new_ts, frame_id});
        less_k_map[frame_id] = new_ts;
    } else {
        // sz == k_，升级到 more_k
        auto lit = less_k_map.find(frame_id);
        if (lit != less_k_map.end()) {
            less_k_set.erase({lit->second, frame_id});
            less_k_map.erase(frame_id);
        }

        // 更新 more_k
        auto moreit = more_k_map.find(frame_id);
        if (moreit != more_k_map.end()) {
            more_k_set.erase({moreit->second, frame_id});
            more_k_map.erase(frame_id);
        }

        // 插入新的 k-th ts
        size_t kth_ts = node.history_.front();
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
        // 先清理旧位置
        auto lit = less_k_map.find(frame_id);
        if (lit != less_k_map.end()) {
            less_k_set.erase({lit->second, frame_id});
            less_k_map.erase(frame_id);
        }
        auto mit = more_k_map.find(frame_id);
        if (mit != more_k_map.end()) {
            more_k_set.erase({mit->second, frame_id});
            more_k_map.erase(frame_id);
        }

        node.is_evictable_ = true;
        curr_size_++;
        // 剪 history
        while (node.history_.size() > k_) {
            node.history_.pop_front();
        }

        size_t sz = node.history_.size();

        if (sz < k_) {
            size_t last_ts = node.history_.back();
            less_k_set.insert({last_ts, frame_id});
            less_k_map[frame_id] = last_ts;
        } else if (sz >= k_ && !node.history_.empty()) {
            size_t kth_ts = node.history_.front();
            more_k_set.insert({kth_ts, frame_id});
            more_k_map[frame_id] = kth_ts;
        }
    } 
    else {
        node.is_evictable_ = false;
        curr_size_--;
        
        auto lit = less_k_map.find(frame_id);
        if (lit != less_k_map.end()) {
            less_k_set.erase({lit->second, frame_id});
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
    
    // 清 less_k
    auto lit = less_k_map.find(frame_id);
    if (lit != less_k_map.end()) {
        less_k_set.erase({lit->second, frame_id});
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

}  // namespace bustub



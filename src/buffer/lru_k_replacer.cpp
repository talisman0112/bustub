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
    std::vector<frame_id_t> less_k;
    std::vector<frame_id_t> more_k;
    //分组
    for (const auto& [fid, node] : node_store_) {
        if (!node.is_evictable_) continue;
        
        if (node.history_.size() < k_) {
            less_k.push_back(fid);
        } else {
            more_k.push_back(fid);
        }
    }
    //优先踢 < k 的
    if (!less_k.empty()) {
        frame_id_t victim = -1;
        size_t min_ts = SIZE_MAX;
        
        for (auto fid : less_k) {
            size_t ts = node_store_[fid].history_.front();
            if (ts < min_ts) {
                min_ts = ts;
                victim = fid;
            }
        }
        *frame_id = victim;
        node_store_.erase(victim);
        curr_size_--;
        return true;
    }
    //再踢 >= k 的
    if (!more_k.empty()) {
        frame_id_t victim = -1;
        size_t min_k_dist = SIZE_MAX;
        
        for (auto fid : more_k) {
            auto& hist = node_store_[fid].history_;
            auto it = hist.end();
            std::advance(it, -static_cast<int>(k_));
            size_t k_dist = *it;
            
            if (k_dist < min_k_dist) {
                min_k_dist = k_dist;
                victim = fid;
            }
        }
        *frame_id = victim;        
        node_store_.erase(victim);
        curr_size_--;
        return true;
    }
       return false;
 }

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
   std::scoped_lock<std::mutex> lock(latch_);
    if(frame_id<0||static_cast<size_t>(frame_id)>=replacer_size_){
        throw std::exception();
    }
    
    if(node_store_.find(frame_id)==node_store_.end()){ 
        LRUKNode node;
        node.k_=k_;
        node.fid_=frame_id;
        node.history_={current_timestamp_};
        node.is_evictable_=false;
        node_store_.insert({frame_id,node});
    }
    else{
        LRUKNode &node=node_store_[frame_id];
    
        if (node.history_.size()==k_)
        {
            node.history_.pop_front();
        }
        node.history_.push_back(current_timestamp_);
}
    current_timestamp_++;  
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::scoped_lock<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        return; 
    }
    LRUKNode &node=node_store_[frame_id];
    if(!node.is_evictable_&&set_evictable){    
        curr_size_++;
    }
    else if(node.is_evictable_&&!set_evictable){
        curr_size_--;
    }
    node.is_evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    std::scoped_lock<std::mutex> lock(latch_);
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        return; 
    }
    else if (it->second.is_evictable_==false)
    {
        throw std::exception();
    }
    
    else{
        node_store_.erase(it);
        curr_size_--;
    }
}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub


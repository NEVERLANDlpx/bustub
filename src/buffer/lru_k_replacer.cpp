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

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {

}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool { 
   bool flag=false;
     int maxx=0;bool inf=false; int ind=-1;
     for(auto a:node_store_)
     {
        if((a.second).is_evictable_==true) flag=true;
        else continue;
        if((a.second).history_.size()<k_) 
        {
           if(!inf)
           {
             inf=true;
             ind=a.first;
           }
           else
           {
             if((a.second).history_.back()<node_store_[ind].history_.back())
             {
               ind=a.first;
             }
           }
        }
        else if(!inf)
        {
          if(ind==-1) 
          {
             auto it = (a.second).history_.begin();
             std::advance(it, k_-1); // 将迭代器移动k-1个位置，即跳过第k-1个元素
             maxx=current_timestamp_-(*it);
             ind=a.first;
          }
          else 
          {
             auto it = (a.second).history_.begin();
             std::advance(it, k_-1); 
             if(maxx<current_timestamp_-(*it))
             {
               maxx=current_timestamp_-(*it);
               ind=a.first;
             }
          }
        }
     }
       if(flag) { 
       *frame_id=ind;
       //Remove(ind); modified
       }
    return flag;
  
}

#include <cstdio>
void LRUKReplacer::RecordAccess(frame_id_t frame_id,AccessType access_type) {
  if(frame_id>replacer_size_)   throw std::runtime_error("Invalid frame_id: " + std::to_string(frame_id)); 
    //printf("%d\n",frame_id);
    auto iterator = node_store_.find(frame_id);  
    if(iterator == node_store_.end())
     {
      LRUKNode newframe;
      node_store_[frame_id]=newframe;
      newframe.k_=k_;
      newframe.fid_=frame_id;
     }
     current_timestamp_++;
     node_store_[frame_id].history_.push_front(current_timestamp_);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
	//for(auto it:node_store_) printf("%d ",it.first);
	//puts("");
     auto iterator = node_store_.find(frame_id);  
     if(iterator == node_store_.end())
    throw std::runtime_error("Invalid frame_id: " + std::to_string(frame_id)); 
    // latch_.lock();
     if(set_evictable&&node_store_[frame_id].is_evictable_==false) 
     {
      curr_size_++;
      node_store_[frame_id].is_evictable_=set_evictable;
     }
     else if(!set_evictable&&node_store_[frame_id].is_evictable_==true) 
     {
      curr_size_--;
      node_store_[frame_id].is_evictable_=set_evictable;
     } 
    // latch_.lock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if(node_store_[frame_id].is_evictable_==false)
    throw std::runtime_error("Invalid frame_id: " + std::to_string(frame_id)); 
    
   auto iterator = node_store_.find(frame_id);  
   if(iterator == node_store_.end()) return;
   
   //latch_.lock();
   node_store_[frame_id].history_.clear();
   //printf("*\n");
   node_store_.erase(frame_id);
   curr_size_--;
   //latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {     return curr_size_; }

}  // namespace bustub

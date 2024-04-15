//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_directory_page.cpp
//
// Identification: src/storage/page/extendible_htable_directory_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_directory_page.h"

#include <algorithm>
#include <unordered_map>

#include "common/config.h"
#include "common/logger.h"
//add
#include<map>
namespace bustub {

void ExtendibleHTableDirectoryPage::Init(uint32_t max_depth) {
  //throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
  max_depth_=max_depth;
 // printf("(1<<max_depth)-1:%u\n",(1<<max_depth)-1);
    for (auto& id : bucket_page_ids_) {
      id = INVALID_PAGE_ID;
    }
     for (auto& id :local_depths_) {
      id = 0;
    }
    global_depth_=0;
}

auto ExtendibleHTableDirectoryPage::HashToBucketIndex(uint32_t hash) const -> uint32_t { 
  uint32_t local_hash = hash & GetGlobalDepthMask();
 // std::cout<<"HashToBucketIndex"<<hash<<"|"<<(local_hash & GetLocalDepthMask(local_hash))<<std::endl;
 // return (local_hash & GetLocalDepthMask(local_hash));
 return local_hash;
//return 0;
 }

auto ExtendibleHTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) const -> page_id_t { 
  if(bucket_idx>=HTABLE_DIRECTORY_ARRAY_SIZE)  {
    //throw std::out_of_range("Bucket index out of range");  
  //  std::cout<<"INVALID_PAGE_ID"<<INVALID_PAGE_ID<<std::endl;
    return INVALID_PAGE_ID;
  }
  return bucket_page_ids_[bucket_idx];
//return INVALID_PAGE_ID; 
}

void ExtendibleHTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  if(bucket_idx>=HTABLE_DIRECTORY_ARRAY_SIZE)  {
    //throw std::out_of_range("Bucket index out of range");  
    return;
  }
  bucket_page_ids_[bucket_idx]=bucket_page_id;
  //throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) const -> uint32_t { 
 // 获取给定目录索引的分裂镜像索引
  /* if(local_depths_[bucket_idx]==global_depth_) IncrLocalDepth(bucket_idx);*/
  if(bucket_page_ids_[bucket_idx]==INVALID_PAGE_ID) return INVALID_PAGE_ID;
  if(local_depths_[bucket_idx]==0) return 0;
   uint32_t highest_bit = 1 << (local_depths_[bucket_idx] - 1);
        return bucket_idx ^ highest_bit;
//return 0; 
}

auto ExtendibleHTableDirectoryPage::GetGlobalDepth() const -> uint32_t { 
   return global_depth_;
  //return 0;
}

void ExtendibleHTableDirectoryPage::IncrGlobalDepth() {
  //double the directory
   for (int i=0; i<Size(); i++) {
    bucket_page_ids_[Size() + i] = bucket_page_ids_[i];
    local_depths_[Size()+i]=local_depths_[i];
   }
   global_depth_++;
  //throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrGlobalDepth() {
  //if(CanShrink()) 
  global_depth_--;
  for(int i=Size();i<2*Size()-1;i++)
  {
    bucket_page_ids_[i] =INVALID_PAGE_ID;
    local_depths_[i]=0;
  }
  
  //throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

auto ExtendibleHTableDirectoryPage::CanShrink() -> bool { 
   for(int i=0;i<Size();i++)
   {
     if(local_depths_[i]>=global_depth_) return false;
   }
   return true;
//return false; 
}

auto ExtendibleHTableDirectoryPage::Size() const -> uint32_t {
  return (1<<global_depth_);
 //return 0; 
 }

auto ExtendibleHTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) const -> uint32_t { 
   return local_depths_[bucket_idx];
//return 0;
 }

void ExtendibleHTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
   local_depths_[bucket_idx]=local_depth;
  //throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) {
  if(local_depths_[bucket_idx]==global_depth_)
  {
     IncrGlobalDepth();
  }
   local_depths_[bucket_idx]++;
  //throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}

void ExtendibleHTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) {
   local_depths_[bucket_idx]--;
   //DecrGlobalDepth();
  //throw NotImplementedException("ExtendibleHTableDirectoryPage is not implemented");
}
//add func begin
auto ExtendibleHTableDirectoryPage::GetGlobalDepthMask() const -> uint32_t {
    return (1 << global_depth_) - 1;
}

auto ExtendibleHTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx) const -> uint32_t {
    uint32_t local_depth = local_depths_[bucket_idx];
    return (1 << local_depth) - 1;
}
/*
void ExtendibleHTableDirectoryPage::VerifyIntegrity() const {
   std::map<page_id_t,int> m;
    for (uint32_t i = 0; i <Size(); i++) {
        // Invariant 1: All local depths <= global depth
       assert(local_depths_[i] <= global_depth_);
        if(m.find(bucket_page_ids_[i])!=m.end()) m[bucket_page_ids_[i]]++;
        else m[bucket_page_ids_[i]]=1;

   
        // Invariant 3: The LD is the same at each index with the same bucket_page_id
        for (uint32_t j = i + 1; j < Size(); ++j) {
            if (bucket_page_ids_[i]== bucket_page_ids_[j]) {
                 assert(local_depths_[i] == local_depths_[j]);
            }
        }
    }
      // Invariant 2: Each bucket has precisely 2^(GD - LD) pointers pointing to it
      for(int i=0;i<Size();i++)
      assert(m[bucket_page_ids_[i]]==1<<(GetGlobalDepth()-GetLocalDepth()));
}
*/
//add func end
}  // namespace bustub

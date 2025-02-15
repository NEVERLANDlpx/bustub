//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_bucket_page.cpp
//
// Identification: src/storage/page/extendible_htable_bucket_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <optional>
#include <utility>

#include "common/exception.h"
#include "storage/page/extendible_htable_bucket_page.h"

namespace bustub {

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::Init(uint32_t max_size) {
  //throw NotImplementedException("ExtendibleHTableBucketPage not implemented");
  max_size_=max_size;
  size_=0;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Lookup(const K &key, V &value, const KC &cmp) const -> bool {
  
    for(int i=0;i<size_;i++)
    {
      
      if(cmp(array_[i].first,key)==0) 
      {
      value=array_[i].second;
      return true;
      }
    }
    return false;
    // return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Insert(const K &key, const V &value, const KC &cmp) -> bool {

  if(size_==max_size_) return false;

  for(int i=0;i<size_;i++)
  {
    if(cmp(array_[i].first,key)==0)
    { 
      return false;
    }
  }

  array_[size_++]=std::make_pair(key,value);
  return true;
  //return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Remove(const K &key, const KC &cmp) -> bool {
  for(int i=0;i<size_;i++)
  {
    if(!cmp(array_[i].first,key))
    {
      std::swap(array_[i],array_[size_-1]);
      size_--;
      return true;
    }
  }

  return false;
}

template <typename K, typename V, typename KC>
void ExtendibleHTableBucketPage<K, V, KC>::RemoveAt(uint32_t bucket_idx) {
 // throw NotImplementedException("ExtendibleHTableBucketPage not implemented");
 if(bucket_idx>=size_) throw std::out_of_range("bucket index out of range");  
 std::swap(array_[bucket_idx],array_[size_-1]);
 size_--;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::KeyAt(uint32_t bucket_idx) const -> K {
   if(bucket_idx>=size_) throw std::out_of_range("bucket index out of range");  
   return array_[bucket_idx].first;
  //return {};
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::ValueAt(uint32_t bucket_idx) const -> V {
   if(bucket_idx>=size_) throw std::out_of_range("bucket index out of range");  
   return array_[bucket_idx].second;
  //return {};
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::EntryAt(uint32_t bucket_idx) const -> const std::pair<K, V> & {
  if(bucket_idx>=size_) throw std::out_of_range("bucket index out of range");  
   return array_[bucket_idx];
  //return array_[0];
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::Size() const -> uint32_t {
   return size_;
  //return 0;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsFull() const -> bool {
  if(size_==max_size_) return true;
  else return false;
  //return false;
}

template <typename K, typename V, typename KC>
auto ExtendibleHTableBucketPage<K, V, KC>::IsEmpty() const -> bool {
  if(size_) return false;
  else return true;
 // return false;
}

template class ExtendibleHTableBucketPage<int, int, IntComparator>;
template class ExtendibleHTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

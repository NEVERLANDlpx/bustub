//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_extendible_hash_table.cpp
//
// Identification: src/container/disk/hash/disk_extendible_hash_table.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "common/util/hash_util.h"
#include "container/disk/hash/disk_extendible_hash_table.h"
#include "storage/index/hash_comparator.h"
#include "storage/page/extendible_htable_bucket_page.h"
#include "storage/page/extendible_htable_directory_page.h"
#include "storage/page/extendible_htable_header_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

template <typename K, typename V, typename KC>
DiskExtendibleHashTable<K, V, KC>::DiskExtendibleHashTable(const std::string &name, BufferPoolManager *bpm,
                                                           const KC &cmp, const HashFunction<K> &hash_fn,
                                                           uint32_t header_max_depth, uint32_t directory_max_depth,
                                                           uint32_t bucket_max_size)
    : bpm_(bpm),
      cmp_(cmp),
      hash_fn_(std::move(hash_fn)),
      header_max_depth_(header_max_depth),
      directory_max_depth_(directory_max_depth),
      bucket_max_size_(bucket_max_size) {
      //throw NotImplementedException("DiskExtendibleHashTable is not implemented");
      directory_max_depth_=directory_max_depth;
      bucket_max_size_=bucket_max_size;
      index_name_=name;
      BasicPageGuard guard=bpm->NewPageGuarded(&header_page_id_);
      auto header_page=guard.AsMut<ExtendibleHTableHeaderPage>();//将页面指针转换为指向ExtendibleHTableHeaderPage类型的指针
      header_page->Init(header_max_depth_);
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::GetValue(const K &key, std::vector<V> *result, Transaction *transaction) const
    -> bool {
    // 计算键的哈希值  
    //uint32_t hash_val=static_cast<uint32_t>(hash_fn_.GetHash(key));
    uint32_t hash_val=Hash(key);
    ReadPageGuard header_guard=bpm_->FetchPageRead(header_page_id_);
    auto header_page=header_guard.As<const ExtendibleHTableHeaderPage>();
    page_id_t directory_page_id = header_page->GetDirectoryPageId( header_page->HashToDirectoryIndex(hash_val));  
    if (directory_page_id == INVALID_PAGE_ID) {   return false;}  
    ReadPageGuard directory_guard=bpm_->FetchPageRead(directory_page_id);
     auto directory_page=directory_guard.As<const ExtendibleHTableDirectoryPage>();
  
    // 获取桶页ID  
    page_id_t bucket_page_id = directory_page->GetBucketPageId(directory_page->HashToBucketIndex(hash_val));  
 
    if (bucket_page_id == INVALID_PAGE_ID) { 
        return false;  
    } 

    // 获取桶页  
    ReadPageGuard bucket_guard=bpm_->FetchPageRead(bucket_page_id);
     auto bucket_page=bucket_guard.As<const ExtendibleHTableBucketPage<K,V,KC>>();
    //ExtendibleHTableBucketPage<K,V,KC> *bucket_page = reinterpret_cast<ExtendibleHTableBucketPage<K,V,KC>*>(bpm_->FetchPage(bucket_page_id));  
    // 在桶页中查找键  
    V value;
       if(bucket_page->Lookup(key, value, cmp_))
            result->push_back(value);  
  
    return !result->empty(); // 如果结果不为空，说明找到了值 

  //return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Insert(const K &key, const V &value, Transaction *transaction) -> bool {
   //std::cout<<"insert begin";
    uint32_t hash_val=Hash(key);
  //  printf("%u\n",hash_val);
    WritePageGuard header_guard=bpm_->FetchPageWrite(header_page_id_);  
    auto header_page=header_guard.AsMut< ExtendibleHTableHeaderPage>();
    header_guard.Drop();
    uint32_t directory_idx=header_page->HashToDirectoryIndex(hash_val);
    page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);  
    if(directory_page_id==INVALID_PAGE_ID)
    {
       return InsertToNewDirectory(header_page,directory_idx, hash_val, key, value);
    }
    WritePageGuard directory_guard=bpm_->FetchPageWrite(directory_page_id);  
    auto directory_page=directory_guard.AsMut< ExtendibleHTableDirectoryPage>();
    directory_guard.Drop();
    uint32_t bucket_idx=directory_page->HashToBucketIndex(hash_val);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx);  
    if(bucket_page_id==INVALID_PAGE_ID)
    {
      return InsertToNewBucket(directory_page, bucket_idx, key, value);
    }
     WritePageGuard bucket_guard=bpm_->FetchPageWrite(bucket_page_id);
     auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
     bucket_guard.Drop();
     V v;
     
     if(bucket_page->Insert(key,value,cmp_)) return true;
     else if(bucket_page->Lookup(key,v,cmp_)) return false;
    
     //split
     auto local_mask=directory_page->GetLocalDepthMask(bucket_idx);
     if(directory_page->GetLocalDepth(bucket_idx)==directory_max_depth_)  return false;//the table is full
        
     for(int i=0;i<directory_page->Size();i++)
     {
       if(directory_page->GetBucketPageId(i)==bucket_page_id)
       directory_page->IncrLocalDepth(i);
     }

     page_id_t new_page_id;
     BasicPageGuard new_guard=bpm_->NewPageGuarded(&new_page_id);
     WritePageGuard new_page_guard=new_guard.UpgradeWrite();
     auto new_page=new_page_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
     new_guard.Drop();
     new_page_guard.Drop();
     new_page->Init(bucket_max_size_);
     uint32_t new_idx=directory_page->GetSplitImageIndex(bucket_idx);
      // directory_page->SetBucketPageId(new_idx,new_page_id);
     for(int i=new_idx;i<directory_page->Size();i++)
     {
       if( (i&directory_page->GetLocalDepthMask(i) ) ==( new_idx&directory_page->GetLocalDepthMask(i) ) ) directory_page->SetBucketPageId(i,new_page_id);
     }
   
        std::vector<K> tonew;
        std::vector<V> tonew_value;
        for(int i=0;i<bucket_page->Size();i++)
        {
          if( (Hash(bucket_page->KeyAt(i))&directory_page->GetLocalDepthMask(bucket_idx)) != (Hash(bucket_page->KeyAt(i))&local_mask ) )
          {
             tonew.push_back(bucket_page->KeyAt(i));
             tonew_value.push_back(bucket_page->ValueAt(i));
          }
        }
     for(int i=0;i<tonew.size();i++)
     {
      
       new_page->Insert(tonew[i],tonew_value[i],cmp_);
       bucket_page->Remove(tonew[i],cmp_);
     }
     uint32_t now_bucket_idx=directory_page->HashToBucketIndex(hash_val);
     if(now_bucket_idx==bucket_idx) 
     {
       if( bucket_page->Insert(key,value,cmp_) ) return true;
       if( directory_page->GetGlobalDepth() == directory_max_depth_ ) return false;
       return Insert(key,value,transaction);
     }
     else
     { 
       if( new_page->Insert(key,value,cmp_) ) return true;
       if( directory_page->GetGlobalDepth() == directory_max_depth_ ) return false;
       return Insert(key,value,transaction);
     }

//  return false;
}
//add begin
template <typename K, typename V, typename KC>
  auto DiskExtendibleHashTable<K, V, KC>::Hash(K key) const -> uint32_t{
     uint32_t hash_val=static_cast<uint32_t>(hash_fn_.GetHash(key));
     return hash_val;
  }
//add end
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewDirectory(ExtendibleHTableHeaderPage *header, uint32_t directory_idx,
                                                             uint32_t hash, const K &key, const V &value) -> bool {                                                 
  if(directory_idx >= ( 1<<directory_max_depth_) ) return false;
  page_id_t directory_page_id;                                                            
  auto new_guard=bpm_->NewPageGuarded(&directory_page_id);
  auto directory_guard=bpm_->FetchPageWrite(directory_page_id);
  auto directory_page=directory_guard.AsMut< ExtendibleHTableDirectoryPage>();
  directory_guard.Drop();
  directory_page->Init(directory_max_depth_);
  header->SetDirectoryPageId(directory_idx,directory_page_id);
  
  return InsertToNewBucket(directory_page , directory_page->HashToBucketIndex(Hash(key)) , key, value);
  //return false;
}

template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::InsertToNewBucket(ExtendibleHTableDirectoryPage *directory, uint32_t bucket_idx,
                                                          const K &key, const V &value) -> bool {
  page_id_t bucket_page_id;
  auto new_guard=bpm_->NewPageGuarded(&bucket_page_id);
  auto bucket_guard=bpm_->FetchPageWrite(bucket_page_id);
  auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
  bucket_guard.Drop();
  bucket_page->Init(bucket_max_size_);
  directory->SetLocalDepth(bucket_idx,0);
  directory->SetBucketPageId(bucket_idx,bucket_page_id);
 // std::cout<<"insert end"<<std::endl;
  return bucket_page->Insert(key,value,cmp_);
  //return false;
}
/*
template <typename K, typename V, typename KC>
void DiskExtendibleHashTable<K, V, KC>::UpdateDirectoryMapping(ExtendibleHTableDirectoryPage *directory,uint32_t new_bucket_idx, page_id_t new_bucket_page_id,uint32_t new_local_depth, uint32_t local_depth_mask) {

  throw NotImplementedException("DiskExtendibleHashTable is not implemented");
}
template <typename K, typename V, typename KC>
void MigrateEntries(ExtendibleHTableBucketPage<K, V, KC> *old_bucket,ExtendibleHTableBucketPage<K, V, KC> *new_bucket, uint32_t new_bucket_idx,uint32_t local_depth_mask){
                      
}
*/
/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename K, typename V, typename KC>
auto DiskExtendibleHashTable<K, V, KC>::Remove(const K &key, Transaction *transaction) -> bool {
    uint32_t hash_val=Hash(key);
    WritePageGuard header_guard=bpm_->FetchPageWrite(header_page_id_);
    auto header_page=header_guard.AsMut<ExtendibleHTableHeaderPage>();
    header_guard.Drop();
    uint32_t directory_idx=header_page->HashToDirectoryIndex(hash_val);
    page_id_t directory_page_id = header_page->GetDirectoryPageId(directory_idx);
    if(directory_page_id == INVALID_PAGE_ID)  return false;
    WritePageGuard directory_guard=bpm_->FetchPageWrite(directory_page_id);  
    auto directory_page=directory_guard.AsMut< ExtendibleHTableDirectoryPage>();
    directory_guard.Drop();
    uint32_t bucket_idx=directory_page->HashToBucketIndex(hash_val);
    page_id_t bucket_page_id = directory_page->GetBucketPageId(bucket_idx); 
    if(bucket_page_id==INVALID_PAGE_ID) return false;
    WritePageGuard bucket_guard=bpm_->FetchPageWrite(bucket_page_id);
    auto bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
    bucket_guard.Drop();
    if(!bucket_page->Remove(key,cmp_)) return false;
    //remove success
   
    //merge

    if(!bucket_page->IsEmpty()) return true;
  

      page_id_t image_id=directory_page->GetSplitImageIndex(bucket_idx);

      
      WritePageGuard image_guard=bpm_->FetchPageWrite(image_id);
      auto image_page=image_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>(); 
      image_guard.Drop();
      if(image_id==0) return true;
      if(image_id==bucket_page_id) return true;

     if(bucket_page->IsEmpty()&&directory_page->GetLocalDepth(bucket_page_id)==directory_page->GetLocalDepth(image_id))
      {
      
         for(int i=0;i<image_page->Size();i++)  //Migration Entry from image to bucket
         { 
           bool flag;
           flag=bucket_page->Insert(image_page->KeyAt(0),image_page->ValueAt(0),cmp_);
           image_page->RemoveAt(0);
         }
 
        for(int i=0;i<directory_page->Size();i++)  //Update Mapping
        {
           if(directory_page->GetBucketPageId(i)==image_id||directory_page->GetBucketPageId(i)==bucket_page_id)
           {
              directory_page->SetBucketPageId(i,bucket_page_id);
              directory_page->DecrLocalDepth(i);
           }
        }
        if(directory_page->CanShrink()) directory_page->DecrGlobalDepth(); //Shrink
    
        
        bucket_idx=directory_page->HashToBucketIndex(hash_val);  
        bucket_page_id = directory_page->GetBucketPageId(bucket_idx); 
        bucket_guard=bpm_->FetchPageWrite(bucket_page_id);
        bucket_page=bucket_guard.AsMut<ExtendibleHTableBucketPage<K,V,KC>>();
         bucket_guard.Drop();
      }
    return true;
  //return false;
}

template class DiskExtendibleHashTable<int, int, IntComparator>;
template class DiskExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class DiskExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class DiskExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class DiskExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class DiskExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub

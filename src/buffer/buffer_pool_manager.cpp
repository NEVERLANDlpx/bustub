//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager
/*  throw NotImplementedException(
      "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
      "exception line in `buffer_pool_manager.cpp`.");
*/
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }
 
auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * { 
  Page *p=new Page;
  p->page_id_=AllocatePage();
  p->pin_count_=1;
  frame_id_t ind;
  if(!free_list_.empty())
  {
    ind=free_list_.front();
    free_list_.pop_front();
  } 
  else
  {
    bool suc=replacer_->Evict(&ind);
    if(!suc) return nullptr;
    Page* old=&pages_[ind];
        if(old->is_dirty_)//write to disk
       {
         //DiskRequest d;
         //d.is_write_=true;
         //d.page_id_=old->page_id_;
         //d.data_=old->data_;
         //d.callback_.set_value(false);
         //disk_scheduler_->Schedule(std::move(d));
	 //modified
	 auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
   // printf("%d %d\n",ind,pages_[ind].page_id_);
    disk_scheduler_->Schedule({true,old->data_,old->page_id_,std::move(promise)});
    //disk_scheduler_->Schedule({false,new char,0,std::move(promise)});
    future.get();


        }
        page_table_.erase(old->page_id_);
        pages_[ind].ResetMemory();
        //pages_[ind].data_=new char[BUSTUB_PAGE_SIZE];//modified
        pages_[ind].page_id_=p->page_id_;
        pages_[ind].pin_count_=1;
       
     
  }
  replacer_->RecordAccess(ind);
  replacer_->SetEvictable(ind,false);
  
  page_table_[p->page_id_]=ind;
  //printf("|%d|\n",p->page_id_);
  pages_[ind].page_id_=p->page_id_;
  pages_[ind].pin_count_=1;
  
  *page_id=p->page_id_;
  delete p;//modified
  return pages_+ind;//modified
}

#include <cstdio>
auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t ind;
  DiskRequest old;
  if(page_table_.find(page_id)==page_table_.end())//not found in pool
  {
    if(!free_list_.empty())
    {
      ind=free_list_.front();
    }
    else
    {
     bool suc=replacer_->Evict(&ind);
    if(!suc) return nullptr;
    Page &tar=pages_[page_table_[page_id]];
       if(tar.is_dirty_)//write to disk
       {
         //old.is_write_=true;
         //old.page_id_=tar.page_id_;
         //old.data_=tar.data_;
         //old.callback_.set_value(false);
         //disk_scheduler_->Schedule(std::move(old));//write to disk
	 //modified
         tar.is_dirty_=false;auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    //printf("%d %d\n",ind,pages_[ind].page_id_);
    disk_scheduler_->Schedule({true,tar.data_,tar.page_id_,std::move(promise)});
    //disk_scheduler_->Schedule({false,new char,0,std::move(promise)});
    future.get();

        }
    }
    replacer_->SetEvictable(ind,false);
    replacer_->RecordAccess(ind);//modified
    //pages_[ind].ResetMemory();modified
    pages_[ind].page_id_=page_id;
    //pages_[ind].data_=new char[BUSTUB_PAGE_SIZE];modified
    pages_[ind].pin_count_++;//modified
    
    //DiskRequest xin;
    //xin.is_write_=false;
    //xin.page_id_=page_id;
    //xin.data_=pages_[ind].data_;//modified
    //disk_scheduler_->Schedule(std::move(xin));  //read from disk
  //  printf("*\n");
    auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    printf("%d %d\n",ind,pages_[ind].page_id_);
    disk_scheduler_->Schedule({false,pages_[ind].GetData(),pages_[ind].page_id_,std::move(promise)});
    //disk_scheduler_->Schedule({false,new char,0,std::move(promise)});
    future.get();
    page_table_[page_id]=ind;
    printf("f%d %d %sf\n",page_id,ind,pages_[ind].data_);
    //printf("|%d|\n",page_id);
  }
  
 return &(pages_[page_table_[page_id]]);
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
    if(page_table_.find(page_id)==page_table_.end()) return false;
 //printf("%d\n",page_id);
    Page *curr =&pages_[page_table_[page_id]];  
    	//printf("%d\n",curr->pin_count_);
      if(curr->pin_count_<=0) return false;
      curr->pin_count_--;
      curr->is_dirty_=true;
      if(curr->pin_count_==0)//evict 
      {
        replacer_->node_store_[page_table_[page_id]].is_evictable_=true;
      }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool { 
    if(page_id==INVALID_PAGE_ID) return false;
    if(page_table_.find(page_id)==page_table_.end()) return false;//not found in table
    Page *fp=&pages_[page_table_[page_id]];
         //DiskRequest d;
         //d.is_write_=true;
         //d.page_id_=page_id;
         //d.data_=fp->data_;
    auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    //printf("%x\n",pages_[ind].data_);
    disk_scheduler_->Schedule({true,fp->data_,page_id,std::move(promise)});
    future.get();
         //printf("%d f%cf\n",page_id,*fp->data_);
         //d.callback_.set_value(false);modified
         //disk_scheduler_->Schedule(std::move(d));//flush
         //*fp->data_='a';
         //DiskRequest test;
         //test.is_write_=false;
         //test.page_id_=page_id;
         //test.data_=fp->data_;
         //disk_scheduler_->Schedule(std::move(test));
         //printf("%d f%cf\n",page_id,*fp->data_);
         
         pages_[page_table_[page_id]].is_dirty_=false;//unset
  return true; 
}

void BufferPoolManager::FlushAllPages() {
  for(size_t i=0;i<pool_size_;i++)
  {
         DiskRequest d;
         d.is_write_=true;
         d.page_id_=pages_[i].page_id_;
         d.data_=pages_[i].data_;
         d.callback_.set_value(false);
        disk_scheduler_->Schedule(std::move(d));//flush
        pages_[i].is_dirty_=false;//unset
  }
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool { 
    if(page_table_.find(page_id)==page_table_.end()) return true;
     Page *dp=&pages_[page_table_[page_id]];
       if(dp->pin_count_>0) return false;//is pinned
       
         replacer_->node_store_.erase(page_table_[page_id]); //stop track
         free_list_.push_back(page_table_[page_id]);//add to freelist       
         page_table_.erase(page_id);//delete from table
         
         dp->ResetMemory();//reset
         //dp->data_ = new char[BUSTUB_PAGE_SIZE];//modified
         dp->pin_count_=0;
         dp->is_dirty_=false;
         
         DeallocatePage(page_id);
         return true;
       
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { 
 Page *fetched_page = FetchPage(page_id);
    if (!fetched_page) {
        // If fetching fails, return an empty BasicPageGuard
        return BasicPageGuard(this,nullptr);
    }
    
    // Otherwise, return a BasicPageGuard holding the fetched page
    return BasicPageGuard(this,fetched_page);
//return {this, nullptr}; 
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
 // Fetch the page using FetchPage function with read latch
    Page *fetched_page = FetchPage(page_id);
    if (!fetched_page) {
        // If fetching fails, return an empty ReadPageGuard
        return ReadPageGuard(this,nullptr);
    }
    
    // Otherwise, return a ReadPageGuard holding the fetched page
    return ReadPageGuard(this,fetched_page);
 //return {this, nullptr}; 
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { 
 // Fetch the page using FetchPage function with write latch
    Page *fetched_page = FetchPage(page_id);
    if (!fetched_page) {
        // If fetching fails, return an empty WritePageGuard
        return WritePageGuard(this,nullptr);
    }
    
    // Otherwise, return a WritePageGuard holding the fetched page
    return WritePageGuard(this,fetched_page);
//return {this, nullptr};
 }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { 
  Page *new_page = NewPage(page_id);
    if (!new_page) {
        // If allocation fails, return an empty BasicPageGuard
        return BasicPageGuard(this,nullptr);
    }
    
    // Otherwise, return a BasicPageGuard holding the new page
    return BasicPageGuard(this,new_page);
//return {this, nullptr};
 }

}  // namespace bustub

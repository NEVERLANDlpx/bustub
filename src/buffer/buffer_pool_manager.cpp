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
  // throw NotImplementedException(
  //   "BufferPoolManager is not implemented yet. If you have finished implementing BPM, please remove the throw "
  //   "exception line in `buffer_pool_manager.cpp`.");

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
  latch_.lock();
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
    if(!suc) 
    {
    delete p;
    p=nullptr;
    latch_.unlock();
    return nullptr;
    }
  }
   Page* old=&pages_[ind];
   if(old->is_dirty_)//write to disk
   {
    auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    disk_scheduler_->Schedule({true,old->data_,old->page_id_,std::move(promise)});
    future.get();//阻塞当前线程，直到对应的异步操作完成
    }
    page_table_.erase(old->page_id_);
    pages_[ind].ResetMemory();
    pages_[ind].page_id_=p->page_id_;
    pages_[ind].pin_count_=1;
       
    replacer_->RecordAccess(ind);
    replacer_->SetEvictable(ind,false);
  
    page_table_[p->page_id_]=ind;
    pages_[ind].page_id_=p->page_id_;
  
    *page_id=p->page_id_;
    delete p;
    p=nullptr;
    latch_.unlock();
  return &pages_[ind];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  frame_id_t ind;
  latch_.lock();
  if(page_table_.find(page_id)==page_table_.end())//not found in pool
  {
    if(!free_list_.empty())
    {
      ind=free_list_.front();
       free_list_.pop_front();
    }
    else
    {
     bool suc=replacer_->Evict(&ind);
      if(!suc) 
      {
        return nullptr;
      }
    }
    Page* old=&pages_[ind];
    if(old->is_dirty_)//write to disk
   {
    auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    disk_scheduler_->Schedule({true,old->data_,old->page_id_,std::move(promise)});
    future.get();
   }
    replacer_->SetEvictable(ind,false);
    replacer_->RecordAccess(ind);
    page_table_.erase(pages_[ind].GetPageId());
    pages_[ind].ResetMemory();
    pages_[ind].page_id_=page_id;
    pages_[ind].pin_count_=1;
    
    auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    disk_scheduler_->Schedule({false,pages_[ind].GetData(),pages_[ind].page_id_,std::move(promise)});
    future.get();
    
    page_table_[page_id]=ind;
  }
  else
  {

    replacer_->RecordAccess(page_table_[page_id]);  // calc replacer
    replacer_->SetEvictable(page_table_[page_id], false);
    pages_[page_table_[page_id]].pin_count_++;  // record pin_count_
  }
  latch_.unlock();
 return &(pages_[page_table_[page_id]]);
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
   latch_.lock();
    if(page_table_.find(page_id)==page_table_.end()) 
    {
    latch_.unlock();
    return false;
    }
    Page *curr =&pages_[page_table_[page_id]];  
    if(curr->pin_count_<=0) 
    {
      latch_.unlock();
      return false;
    }
    curr->pin_count_--;
    if(!curr->is_dirty_&&is_dirty) curr->is_dirty_=is_dirty;
    if(curr->pin_count_==0)//evict 
    {
      replacer_->SetEvictable(page_table_[page_id],true);
    }
    latch_.unlock();
    
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
    if(page_id==INVALID_PAGE_ID) 
    {
    return false;
    }
    if(page_table_.find(page_id)==page_table_.end()) 
    {
    return false;//not found in table
    }
    latch_.lock();
    Page *fp=&pages_[page_table_[page_id]];

    auto promise=disk_scheduler_->CreatePromise();
    auto future=promise.get_future();
    disk_scheduler_->Schedule({true,fp->data_,page_id,std::move(promise)});
    future.get();

    pages_[page_table_[page_id]].is_dirty_=false;//unset
    latch_.unlock();
  return true; 
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();
  for (auto id : page_table_) {
    Page *fp=&pages_[id.second];  // flush all;
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    disk_scheduler_->Schedule({true, fp->data_, fp->page_id_, std::move(promise)});
    future.get();
    fp->is_dirty_ = false;
  }
  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
    latch_.lock();
    if(page_table_.find(page_id)==page_table_.end()) 
    {
    latch_.unlock();
    return true;
    }
     Page *dp=&pages_[page_table_[page_id]];
     if(dp->pin_count_>0) 
     {
       latch_.unlock();
       return false;//is pinned
     }

     replacer_->Remove(page_table_[page_id]); //stop track
     free_list_.push_back(page_table_[page_id]);//add to freelist       
     page_table_.erase(page_id);//delete from table
         
     dp->ResetMemory();//reset
     dp->pin_count_=0;
     dp->is_dirty_=false;
         
     DeallocatePage(page_id);
     latch_.unlock();
         return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard {
 Page *fetched_page = FetchPage(page_id);
    return {this,fetched_page};
//return {this, nullptr}; 
}

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard {
 Page *fetched_page = FetchPage(page_id);
    return {this,fetched_page};
}

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard {
 Page *fetched_page = FetchPage(page_id);
    return {this,fetched_page};
}

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard {
  Page *fetched_page = NewPage(page_id);
    return {this,fetched_page};
}

}  // namespace bustub

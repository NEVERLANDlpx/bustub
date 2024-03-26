//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
 /* throw NotImplementedException(
      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
      "throw exception line in `disk_scheduler.cpp`.");*/

  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  request_queue_.Put(std::nullopt); 
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
  
}

void DiskScheduler::Schedule(DiskRequest r) {
   
  request_queue_.Put(std::make_optional(std::move(r))); 
  
}

void DiskScheduler::StartWorkerThread() {
 
  while (true) {  
    std::optional<DiskRequest> a = request_queue_.Get();  
    if (a==std::nullopt) {  
      // 如果遇到 std::nullopt，表示队列为空，停止遍历  
      break;  
    }  
    // 处理有效的 DiskRequest 对象  
     // DiskRequest request = *current_request; // 解引用 std::optional 获取 DiskRequest 对象  
    // 根据 request 的内容执行相应的操作，例如：  
    if (a->is_write_) {  
       disk_manager_->WritePage(a->page_id_, a->data_);
      // 处理写请求...  
    } else {  
      disk_manager_->ReadPage(a->page_id_, a->data_);
      // 处理读请求...  
    }  
  
    // 执行完成后，可能需要设置 callback_ 来通知请求的发起者  
    a->callback_.set_value(true); // 假设请求成功完成  
  } 
 
}

}  // namespace bustub

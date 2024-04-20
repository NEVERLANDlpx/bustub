//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_htable_header_page.cpp
//
// Identification: src/storage/page/extendible_htable_header_page.cpp
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/extendible_htable_header_page.h"

#include "common/exception.h"

namespace bustub {

void ExtendibleHTableHeaderPage::Init(uint32_t max_depth) {
  //throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
  max_depth_=max_depth;
   for (auto& id : directory_page_ids_) {
      id =0;
    }
}

auto ExtendibleHTableHeaderPage::HashToDirectoryIndex(uint32_t hash) const -> uint32_t { 
  // std::cout<<"HashToDirectoryIndex"<<((hash >> (32 - max_depth_)) & ((1 << max_depth_) - 1))<<std::endl;
    if(32-max_depth_==32) return 0;
    return hash >> (32 - max_depth_);
//return 0; 
}

auto ExtendibleHTableHeaderPage::GetDirectoryPageId(uint32_t directory_idx) const -> uint32_t { 
  if (directory_idx >= MaxSize()) {  
    // 抛出异常或返回错误值  
    //throw std::out_of_range("Directory index out of range");  
//    printf("%u\n",directory_idx);
  //   std::cout<<"header_INVALID_PAGE_ID"<<INVALID_PAGE_ID<<std::endl;
    return INVALID_PAGE_ID;
  }  
   
   return directory_page_ids_[directory_idx];
//return 0; 
}

void ExtendibleHTableHeaderPage::SetDirectoryPageId(uint32_t directory_idx, page_id_t directory_page_id) {
  //throw NotImplementedException("ExtendibleHTableHeaderPage is not implemented");
   if (directory_idx >= MaxSize()) {  
    // 抛出异常或返回错误  
    //throw std::out_of_range("Directory index out of range");  
    return ;
  }  
   directory_page_ids_[directory_idx]=directory_page_id;
}


auto ExtendibleHTableHeaderPage::MaxSize() const -> uint32_t { 
   return 1<<max_depth_;
//return 0; 
}

}  // namespace bustub;

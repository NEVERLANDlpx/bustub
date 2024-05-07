//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    //: AbstractExecutor(exec_ctx) {}
    :AbstractExecutor(exec_ctx),
     plan_(plan),
     table_info_(exec_ctx_->GetCatalog()->GetTable(plan->GetIndexOid())),
     index_info_{exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_)},
     htable_{dynamic_cast<HashTableIndexForTwoIntegerColumn *>(index_info_->index_.get())},
     iter_(std::make_unique<TableIterator>(table_info_->table_->MakeIterator())) {
    std::cout<<"haha"<<std::endl;
}
     

void IndexScanExecutor::Init() { 
   //throw NotImplementedException("IndexScanExecutor is not implemented");
   if(plan_->filter_predicate_!=nullptr){
     const auto *right_expr =dynamic_cast<const ConstantValueExpression *>(plan_->filter_predicate_->children_[1].get());
    Value v = right_expr->val_;
    htable_->ScanKey(Tuple{{v}, index_info_->index_->GetKeySchema()}, &rids_, exec_ctx_->GetTransaction());
    rid_iter_ = rids_.begin();
    }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  std::cout<<"start"<<std::endl;
  if(plan_->filter_predicate_!=nullptr)
  {
    if (rid_iter_ != rids_.end()) {
      *rid = *rid_iter_;
      *tuple = table_info_->table_->GetTuple(*rid).second;
      rid_iter_++;
      return true;
    }
    return false;
   }
   
  if ((*iter_).IsEnd()) {
    return false;
  }
  *rid = (*iter_).GetRID();
  *tuple = table_info_->table_->GetTuple(*rid).second;
  ++(*iter_);

  return true;
    
//return false; 
}

}  // namespace bustub

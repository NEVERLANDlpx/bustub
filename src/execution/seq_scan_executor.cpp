//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
    //: AbstractExecutor(exec_ctx) {}
      : AbstractExecutor(exec_ctx), plan_(plan),
      table_info_(exec_ctx_->GetCatalog()->GetTable(plan->GetTableOid())),
      iter_(std::make_unique<TableIterator>(table_info_->table_->MakeIterator())) {
    //this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  //table information     
 }

void SeqScanExecutor::Init() { 
   // throw NotImplementedException("SeqScanExecutor is not implemented"); 
    /*
    首先从执行器上下文中获取要扫描的表格信息。
   然后从表格信息中获取表格堆实例，并使用该实例创建一个迭代器以遍历表格。
    */
 //
  //table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  //table information
  //iter_=make_unique<TableIterator>(table_info_->table_->MakeIterator());
}


auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  /*
    用于产生顺序扫描的下一个元组。
   如果迭代器已经指向了表格的末尾，则返回 false，表示没有更多的元组了。
   如果迭代器尚未指向表格的末尾，则从迭代器获取当前位置的元组和 RID。
  将迭代器指向下一个位置，以便下一次调用 Next 时能够获取下一个元组。
   返回 true，表示成功获取了一个元组。
 */
  while(!iter_->IsEnd() )  {
  if((iter_->GetTuple().first).is_deleted_) 
    {  ++(*iter_); continue;  }

    *tuple =iter_->GetTuple().second; 
    *rid = iter_->GetRID();
    ++(*iter_);
    if(plan_->filter_predicate_!=nullptr)
    {
      if (plan_->filter_predicate_->Evaluate(tuple, plan_->OutputSchema()).GetAs<bool>()) 
      {
        return true;
      }
    } 
    else 
    {
      return true;
    }  
  }
  return false;
 

  //return false;
}

}  // namespace bustub

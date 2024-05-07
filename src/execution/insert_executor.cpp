//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
   // : AbstractExecutor(exec_ctx) {
    : AbstractExecutor(exec_ctx), 
    plan_{plan}, 
    child_executor_{std::move(child_executor) } {
    this->table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  //table information    
}

void InsertExecutor::Init() { 
//throw NotImplementedException("InsertExecutor is not implemented");
   child_executor_->Init();   

}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  
   if(isDone) return false;
    Tuple todo_tuple;
    RID emit_rid;
    int count = 0;
 
    while (child_executor_->Next(&todo_tuple, &emit_rid)) 
    {
      // 进行写入/删除 
    std::optional<RID> result= table_info_->table_->InsertTuple({INVALID_TXN_ID,false}, todo_tuple);
    if(result.has_value())  
    { 
    // 插入成功，可以使用 result.value() 来获取 RID  
    //*rid = result.value();  
    // 更新索引

     indexes_info_= exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
     for (auto index : indexes_info_) {
     index->index_->InsertEntry(todo_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_,index->index_->GetKeyAttrs()),result.value(), exec_ctx_->GetTransaction() );
      } 
      count++;
     }
     
    }
    std::vector<Value> values{{TypeId::INTEGER, count}};
    *tuple = Tuple{values, &GetOutputSchema()};
    isDone = true;
     return true;     
                 
// return false; 
}

}  // namespace bustub

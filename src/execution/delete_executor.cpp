//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    //: AbstractExecutor(exec_ctx) {
     : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
      this->table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());  //table information   
 }

void DeleteExecutor::Init() { 
  //throw NotImplementedException("DeleteExecutor is not implemented"); 
   child_executor_->Init();
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
   if(isDone) return false;
    Tuple todo_tuple;
    RID emit_rid;
    int count = 0;

    while (child_executor_->Next(&todo_tuple, &emit_rid)) 
    {
      // 进行删除
    TupleMeta new_meta=table_info_->table_->GetTupleMeta(emit_rid);
    new_meta.is_deleted_=true;
    table_info_->table_->UpdateTupleMeta(new_meta,emit_rid);
    // 更新索引
     indexes_info_= exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
     for (auto index : indexes_info_) {
     Tuple key=todo_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_,index->index_->GetKeyAttrs());
     index->index_->DeleteEntry(todo_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_,index->index_->GetKeyAttrs()),key.GetRid(), exec_ctx_->GetTransaction() );
      } 
      count++;
    }
     std::vector<Value> values{{TypeId::INTEGER, count}};
    *tuple = Tuple{values, &GetOutputSchema()};
    isDone = true;
   
     return true;   
  //return false; 
}

}  // namespace bustub

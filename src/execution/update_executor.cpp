//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
   // : AbstractExecutor(exec_ctx) {
    : AbstractExecutor(exec_ctx), plan_{plan}, child_executor_{std::move(child_executor)} {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
   this->table_info_ = this->exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
}


void UpdateExecutor::Init() { 
//throw NotImplementedException("UpdateExecutor is not implemented"); 
  child_executor_->Init();
}

auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
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
      
    std::vector<Value> values{};
    values.reserve(child_executor_->GetOutputSchema().GetColumnCount());
    for (const auto &expr : plan_->target_expressions_) {
      values.push_back(expr->Evaluate(&todo_tuple, child_executor_->GetOutputSchema()));
    }

    auto to_update_tuple = Tuple{values, &child_executor_->GetOutputSchema()};
      //进行写入
      std::optional<RID> result= table_info_->table_->InsertTuple({INVALID_TXN_ID,false}, to_update_tuple);
      if(result.has_value())  
      { 
      // 更新索引
       indexes_info_= exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);
       for (auto index : indexes_info_) {
       index->index_->InsertEntry(to_update_tuple.KeyFromTuple(table_info_->schema_, index->key_schema_,index->index_->GetKeyAttrs()),result.value(), exec_ctx_->GetTransaction() );
        }   
      } 
      count++;
    }

     std::vector<Value> values{{TypeId::INTEGER, count}};
    *tuple = Tuple{values, &GetOutputSchema()};
    isDone = true;
   
     return true;        
                 
 // return false;
 }

}  // namespace bustub

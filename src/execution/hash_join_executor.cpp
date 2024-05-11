//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
  //  : AbstractExecutor(exec_ctx) {
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_child)),
      right_executor_(std::move(right_child)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
  
}
//Init()将左表中的tuple加入哈希表
//hash a tuple with multiple attributes in order to construct a unique key
void HashJoinExecutor::Init() { 
 //throw NotImplementedException("HashJoinExecutor is not implemented");
  left_executor_->Init();
  right_executor_->Init();
  Tuple todo_tuple;
  RID emit_rid;
  while (right_executor_->Next(&todo_tuple, &emit_rid)) 
  {
    std::vector<Value> vals;
    for (auto expr : plan_->right_key_expressions_) 
    {
      vals.push_back(expr->Evaluate(&todo_tuple, right_executor_->GetOutputSchema()));
    }
    HashjoinKey key{vals};
    hj_table_[key].push_back(todo_tuple);
  }
  while (left_executor_->Next(&todo_tuple, &emit_rid)) 
  {
    std::vector<Value> vals;
    for (auto expr : plan_->left_key_expressions_) 
    {
      vals.push_back(expr->Evaluate(&todo_tuple, left_executor_->GetOutputSchema()));
    }
    HashjoinKey key{vals};   
    if (hj_table_.count(key) ) //can find in table 
    {
      auto tuples = hj_table_[key];
      for (auto tuple: tuples) 
      {
        std::vector<Value> tuple_vals;
        for (int i=0;i<left_executor_->GetOutputSchema().GetColumnCount(); i++) 
        {
          tuple_vals.push_back(todo_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (int i=0;i<right_executor_->GetOutputSchema().GetColumnCount(); i++) 
        {
          tuple_vals.push_back(tuple.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        results_.push_back(Tuple{tuple_vals, &GetOutputSchema()});
      }
    }
    else if (plan_->GetJoinType() == JoinType::LEFT) 
    {
 
        std::vector<Value> tuple_vals;
        for (int i=0;i< left_executor_->GetOutputSchema().GetColumnCount(); i++) 
        {
          tuple_vals.push_back(todo_tuple.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (int i=0;i<right_executor_->GetOutputSchema().GetColumnCount(); i++) 
        {
          tuple_vals.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        results_.push_back(Tuple{tuple_vals, &GetOutputSchema()});
    } 

  }
  idx_=0;
}
//Next()有两层循环：一层是扫描右表，一层是扫描哈希表中右表对应的tuple集合（因为左表可能有tuple对应的key相等）
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
   if(idx_>=results_.size()) return false;
   *tuple=results_[idx_];
   idx_++;
   return true;
}

}  // namespace bustub

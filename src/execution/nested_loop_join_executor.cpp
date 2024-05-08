//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() { 
//throw NotImplementedException("NestedLoopJoinExecutor is not implemented"); 
  left_executor_->Init();
  right_executor_->Init();
  Tuple tuple;
  RID emit_rid;
   while (right_executor_->Next(&tuple, &emit_rid)) {
    right_tuples_.push_back(tuple);
  }
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
 RID emit_rid;
  bool flag=false;
  while(true)
  {
    if(rend_)
    {
      if(!left_executor_->Next(&left_now_,&emit_rid)) break;
    }  //have done
    if(rend_) 
    {
      rend_=false;
      rind_=0;
      flag_=false;
    }
    std::vector<Value> values;
    if(right_tuples_.size()==0){rend_=true;}
    for(auto i=rind_;i<right_tuples_.size();i++)
    {
      if(i==right_tuples_.size()-1) 
      {rend_=true;}
      auto result = plan_->Predicate()->EvaluateJoin(&left_now_, left_executor_->GetOutputSchema(),& right_tuples_[i],right_executor_->GetOutputSchema());
 
      if(result.IsNull()||!result.GetAs<bool>()) continue;  //cannot match
      flag=true;//left has matched right
      flag_=true;
      for(auto id=0;id<left_executor_->GetOutputSchema().GetColumnCount();id++) 
      {
        values.push_back(left_now_.GetValue(&left_executor_->GetOutputSchema(),id));
      }
      for(auto id=0;id<right_executor_->GetOutputSchema().GetColumnCount();id++)
      {
        values.push_back(right_tuples_[i].GetValue(&right_executor_->GetOutputSchema(),id));
      }      
      rind_=i+1;
      break;   //generate tuple done
    }
 
    if(!flag_&&plan_->GetJoinType() == JoinType::LEFT) //remain left and let right null
    {
      flag=true;
      for(auto id=0;id<left_executor_->GetOutputSchema().GetColumnCount();id++) 
      {
        values.push_back(left_now_.GetValue(&left_executor_->GetOutputSchema(),id));
      }
      for (uint32_t id= 0; id < right_executor_->GetOutputSchema().GetColumnCount(); id++) {
        values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(id).GetType()));
      }      
    }
    if(flag)
    {
      *tuple=Tuple(values,&GetOutputSchema());
      return true;
    }
  }
  return false;    
   //return false; 
}

}  // namespace bustub

//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    //: AbstractExecutor(exec_ctx) {}
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() { 
//throw NotImplementedException("LimitExecutor is not implemented"); 
   child_executor_->Init();
   count=0;
}

auto LimitExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(count>=plan_->GetLimit()) return false;
  if(child_executor_->Next(tuple,rid)) //success emit
  {
    count++;
    return true;
  }
  return false;
  //return false;   
}

}  // namespace bustub

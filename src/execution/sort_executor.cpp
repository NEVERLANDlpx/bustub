#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    //: AbstractExecutor(exec_ctx) {}
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)){}

void SortExecutor::Init() { 
//throw NotImplementedException("SortExecutor is not implemented"); 
   child_executor_->Init();
   RID emit_rid; Tuple todo_tuple;
   while(child_executor_->Next(&todo_tuple,&emit_rid) )
   {
     tuples_.push_back(todo_tuple);
   }
   id_=0;
   
   std::sort(tuples_.begin(), tuples_.end(), [this](const Tuple &a, const Tuple &b) {
    for (auto [order_by_type, expr] : plan_->GetOrderBy()) {
         auto result_a = expr->Evaluate(&a, child_executor_->GetOutputSchema());
         auto result_b = expr->Evaluate(&b, child_executor_->GetOutputSchema());
        switch (order_by_type) {
            case OrderByType::ASC:
                if (static_cast<bool> (result_a.CompareLessThan(result_b)) ) return true;
                if (static_cast<bool> (result_a.CompareGreaterThan(result_b)) ) return false;
                break;
            case OrderByType::DESC:
                if (static_cast<bool> (result_a.CompareGreaterThan(result_b)) ) return true;
                if (static_cast<bool> (result_a.CompareLessThan(result_b)) ) return false;
                break;
            case OrderByType::INVALID:
            case OrderByType::DEFAULT:
                // Assuming DEFAULT as ASCENDING for simplicity
                if (static_cast<bool> (result_a.CompareLessThan(result_b)) ) return true;
                if (static_cast<bool> (result_a.CompareGreaterThan(result_b)) ) return false;
                break;
        }
   
    }
    return false;
    //UNREACHABLE("doesn't support duplicate key");
    });
  
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
    if(id_>=tuples_.size()) return false;
    *tuple=tuples_[id_];
    *rid=tuples_[id_].GetRid();
    id_++;
    return true;
//return false; 
}

}  // namespace bustub

#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
   // : AbstractExecutor(exec_ctx) {}
   : AbstractExecutor(exec_ctx),
     plan_(plan),
     child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() { 
//throw NotImplementedException("TopNExecutor is not implemented"); 
  child_executor_->Init();
  
auto cmp = [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &a, const Tuple &b) {
    for (const auto &[order_by_type, expr] : order_bys) {
        auto result_a = expr->Evaluate(&a, schema);
        auto result_b = expr->Evaluate(&b, schema);

        switch (order_by_type) {
        case OrderByType::ASC:
            if (static_cast<bool>(result_a.CompareLessThan(result_b)))  return true;
            if (static_cast<bool>(result_a.CompareGreaterThan(result_b))) return false;
            break;
        case OrderByType::DESC:
            if (static_cast<bool>(result_a.CompareGreaterThan(result_b))) return true;
            if (static_cast<bool>(result_a.CompareLessThan(result_b))) return false;
            break;
        case OrderByType::INVALID:
        case OrderByType::DEFAULT:
            if (static_cast<bool> (result_a.CompareLessThan(result_b)) ) return true;
            if (static_cast<bool> (result_a.CompareGreaterThan(result_b)) ) return false;
            break;
        }
    }
    return false;
};
    std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> topn_heap(cmp);
    Tuple todo_tuple; RID emit_rid;
    while(child_executor_->Next(&todo_tuple,&emit_rid))
    {
       topn_heap.push(todo_tuple);
       if(topn_heap.size()>plan_->GetN()) topn_heap.pop();
    }
    
    while(!topn_heap.empty())
    {
      tuples_.push_back(topn_heap.top());
      topn_heap.pop();
    }
  
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
   if(!tuples_.size()) return false;
   *tuple=tuples_.back();
   tuples_.pop_back();
   *rid=tuple->GetRid();
   return true;
//return false; 
}

auto TopNExecutor::GetNumInHeap() -> size_t { 
    return tuples_.size();
//throw NotImplementedException("TopNExecutor is not implemented"); 
};

}  // namespace bustub

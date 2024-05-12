#include "execution/executors/window_function_executor.h"
#include "execution/plans/window_plan.h"
#include "storage/table/tuple.h"
//add
#include "type/value_factory.h"
#include <unordered_map>
namespace bustub {

WindowFunctionExecutor::WindowFunctionExecutor(ExecutorContext *exec_ctx, const WindowFunctionPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void WindowFunctionExecutor::doit(std::vector<Value> &ret) {                
   std::unordered_map<CombineKey, std::vector<uint32_t> > map_;            
  auto cmp = [=](const std::pair<Tuple, uint32_t> &a, const std::pair<Tuple, uint32_t> &b) -> bool {
    for (auto tuple_with_id : func.order_by_) {
      auto result_a = tuple_with_id.second->Evaluate(&a.first, GetOutputSchema());
      auto result_b = tuple_with_id.second->Evaluate(&b.first, GetOutputSchema());
      switch (tuple_with_id.first) 
      {
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
  };
   std::sort(tuple_with_id.begin(), tuple_with_id.end(), cmp);

  for (int i = 0; i < tuple_with_id.size(); i++) 
  {
    std::vector<Value> keys;
    for (auto expr : func.partition_by_) 
    {
      keys.push_back(expr->Evaluate(&tuple_with_id[i].first, child_executor_->GetOutputSchema()));
    }
    CombineKey key{keys};
    map_[key].push_back(i);
    
  }  //partition/group

  for (auto key_pair : map_) 
  {
    std::vector<Value> vals;
    Value current = func.type_ == WindowFunctionType::CountStarAggregate ? ValueFactory::GetIntegerValue(0) : ValueFactory::GetNullValueByType(TypeId::INTEGER);
    for (int index : key_pair.second) {
        auto& tuple = tuple_with_id[index].first;
        Value evaluated = func.function_->Evaluate(&tuple, child_executor_->GetOutputSchema());
        bool isNullEvaluated = evaluated.IsNull();

        switch (func.type_) {
            case WindowFunctionType::Rank:
                current = current.IsNull() ? ValueFactory::GetIntegerValue(1) : current.Add(ValueFactory::GetIntegerValue(1));
                break;
            case WindowFunctionType::MinAggregate:
                if (!isNullEvaluated) current = current.IsNull() ? evaluated : current.Min(evaluated);
                break;
            case WindowFunctionType::MaxAggregate:
                if (!isNullEvaluated) current = current.IsNull() ? evaluated : current.Max(evaluated);
                break;
            case WindowFunctionType::SumAggregate:
                if (!isNullEvaluated) current = current.IsNull() ? evaluated : current.Add(evaluated);
                break;
            case WindowFunctionType::CountAggregate:
                if (!isNullEvaluated) current = current.IsNull() ? ValueFactory::GetIntegerValue(1) : current.Add(ValueFactory::GetIntegerValue(1));
                break;
            case WindowFunctionType::CountStarAggregate:
                current = current.Add(ValueFactory::GetIntegerValue(1));
                break;
        }
        vals.push_back(current);
    }
    int size = vals.size();
   if (func.type_ != WindowFunctionType::Rank) 
   {
   // 逆序处理所有操作，仅遍历一次
    for (int i =size-2;i>=0;i--) 
    {
      if(cmp(tuple_with_id[key_pair.second[i]], tuple_with_id[key_pair.second[i + 1]]) ) continue;
      if(cmp(tuple_with_id[key_pair.second[i + 1]], tuple_with_id[key_pair.second[i]])) continue;
      vals[i]=vals[i+1];
    }

   }
  else 
  {
  // 正序处理
    for (int i=1;i< size;i++) 
   {
   //如果相邻元素相等，当前元素的值设为前一个元素的值。
   if(cmp(tuple_with_id[key_pair.second[i]], tuple_with_id[key_pair.second[i - 1]])) continue;
   if(cmp(tuple_with_id[key_pair.second[i - 1]], tuple_with_id[key_pair.second[i]])) continue;
    vals[i]=vals[i-1];
   }
  }
      for (int i=size-1;i>= 0;i--) 
    {
      ret[tuple_with_id[key_pair.second[i]].second] = vals[i];
    }
  }
}

void WindowFunctionExecutor::Init() {
  // throw NotImplementedException("WindowFunctionExecutor is not implemented");
  results_.resize(plan_->columns_.size());
  child_executor_->Init();
  Tuple todo_tuple;
  RID emit_rid;
  int cnt=0;
  while (child_executor_->Next(&todo_tuple, &emit_rid)) 
  {
    tuple_with_id.push_back(std::make_pair(todo_tuple, cnt));
    cnt++;
    rids_.push_back(emit_rid);
  }
  //save the tuple information
 
  
  for (int i=0;i<plan_->columns_.size();i++) 
  {
    results_[i].resize(cnt);
    if (plan_->window_functions_.find(i) == plan_->window_functions_.end()) 
    {
      for (int j=0;j<tuple_with_id.size();j++) 
      {
        auto geti=tuple_with_id[j];
        results_[i][geti.second] = plan_->columns_[i]->Evaluate(&geti.first, child_executor_->GetOutputSchema());
      }
    }
    else
    {
        if ( plan_->window_functions_.find(i)->second.order_by_.size()) 
        {
          sort_by_ =  plan_->window_functions_.find(i)->second.order_by_;
        }
        func= plan_->window_functions_.find(i)->second;
        doit(results_[i]);
    } 

  }
  for (int i=0;i<cnt;i++) 
  {
    std::vector<Value> values;
    for (int j = 0; j < plan_->columns_.size(); j++) 
    {
      values.push_back(results_[j][i]);
    }
    ans_.push_back(std::make_pair(Tuple{values, &GetOutputSchema()}, rids_[i]));
  }

    std::sort(ans_.begin(), ans_.end(), [=](const std::pair<Tuple, RID> &a, const std::pair<Tuple, RID> &b) -> bool {
          for (auto tuple_with_id : sort_by_) 
          {
               auto result_a = tuple_with_id.second->Evaluate(&a.first, GetOutputSchema());
               auto result_b = tuple_with_id.second->Evaluate(&b.first, GetOutputSchema());
               switch (tuple_with_id.first) 
               {
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
      });

  id=0;
}

auto WindowFunctionExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if(id>=ans_.size()) return false;
  *tuple=ans_[id].first;
  *rid=ans_[id].second;
  id++;
  return true;

}
}  // namespace bustub

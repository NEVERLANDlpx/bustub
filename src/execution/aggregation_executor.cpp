//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
   // : AbstractExecutor(exec_ctx) {}
    : AbstractExecutor(exec_ctx),
     plan_(plan),
     child_executor_ (std::move(child_executor)),
     aht_(plan_->aggregates_, plan_->agg_types_),  //agg_exprs_和agg_types_分别表示聚合表达式的数量和类型。
     aht_iterator_(aht_.Begin()){}

void AggregationExecutor::Init() {
   child_executor_->Init();
   Tuple todo_tuple;
   RID rid;
   while(child_executor_->Next(&todo_tuple,&rid))
   {
     aht_.InsertCombine(MakeAggregateKey(&todo_tuple),MakeAggregateValue(&todo_tuple));
   }
   if(aht_.Begin()==aht_.End())
   {
     //aht_.ht_.insert({agg_key, aht_.GenerateInitialAggregateValue()});
     //aht_.InsertCombine( {keys} , aht_.GenerateInitialAggregateValue() );
   }
   aht_iterator_=aht_.Begin();
}

auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  if(aht_iterator_==aht_.End())  {
  	if(aht_.Begin()==aht_.End()&&plan_->agg_types_.size()==1&&plan_->group_bys_.size()==0){
  		printf("*\n");
  		std::vector<Value> values;
  		auto tmp=aht_.GenerateInitialAggregateValue().aggregates_;
  		values.insert(values.end(), tmp.begin(), tmp.end());
  		*tuple = Tuple{values, &GetOutputSchema()};
  		
  		
     		std::vector<Value> keys;
     		AggregateKey agg_key{keys};
  		aht_.InsertCombine({keys},aht_.GenerateInitialAggregateValue());
  		aht_iterator_=aht_.End();
  		return true;
  	}
  	return false;
  }
  std::vector<Value> values;
  values.insert(values.end(), aht_iterator_.Key().group_bys_.begin(), aht_iterator_.Key().group_bys_.end());  //当前位置的键（key）中的分组属性（group_bys）插入到values向量的末尾
  values.insert(values.end(), aht_iterator_.Val().aggregates_.begin(), aht_iterator_.Val().aggregates_.end());  //当前位置的值（value）中的聚合属性（aggregates）插入到values向量的末尾
  *tuple = Tuple{values, &GetOutputSchema()};
  ++aht_iterator_;
  return true;
//return false; 
}

auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub

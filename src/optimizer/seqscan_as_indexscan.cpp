#include "optimizer/optimizer.h"
//add
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
namespace bustub {

bool canopt(const AbstractExpressionRef expr, std::vector<std::pair<uint32_t, AbstractExpressionRef> > &map_) {
  if (expr == nullptr) 
  {
    return false;
  }
  auto *logi_expr = dynamic_cast<const LogicExpression *>(expr.get()); 
  auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
  if (logi_expr != nullptr) 
  {
    if (logi_expr->logic_type_ == LogicType::Or) 
    {
      return canopt(logi_expr->children_[0], map_) && canopt(logi_expr->children_[1], map_);
    }
  } 
  else if (cmp_expr != nullptr) 
  {
    auto *l1 = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[0].get());
    auto *l2 = dynamic_cast<const ConstantValueExpression *>(cmp_expr->children_[0].get());
    auto *r1 = dynamic_cast<const ConstantValueExpression *>(cmp_expr->children_[1].get());
    auto *r2 = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[1].get());
    if (l1 != nullptr && r1 != nullptr) 
    {
      if (l1->GetTupleIdx() == 0) 
      {
        for (auto pair : map_) 
        {
          if (pair.first != l1->GetColIdx()) return false;
          auto now_val= dynamic_cast<const ConstantValueExpression *>(pair.second.get())->val_;
          if (now_val.CompareEquals( r1->val_) == CmpBool::CmpTrue) return true;  
        }
        map_.push_back(std::make_pair(l1->GetColIdx(), cmp_expr->children_[1]));  //cannot
 
      }
    }
    if (l2 != nullptr && r2 != nullptr) 
    {
      if (r2->GetTupleIdx() == 0) 
      {
        for (auto pair: map_) 
        {
          if (pair.first != r2->GetColIdx()) return false;
          auto now_val= dynamic_cast<const ConstantValueExpression *>(pair.second.get())->val_;
          if (now_val.CompareEquals(l2->val_) == CmpBool::CmpTrue) return true;
        }
        map_.push_back(std::make_pair(r2->GetColIdx(), cmp_expr->children_[0]));  //cannot
      }
    }
  }
  return false;
}

auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    auto &old_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    std::vector<std::pair<uint32_t, AbstractExpressionRef> > expr_map;
    if (canopt(old_plan.filter_predicate_, expr_map)) 
    {
      auto name = old_plan.OutputSchema().GetColumn(expr_map[0].first).GetName();  
      auto indexes = catalog_.GetTableIndexes(old_plan.table_name_);
      for (auto index : indexes) 
      {
        if (index->key_schema_.GetColumnCount() == 1 &&old_plan.table_name_ + '.' + index->key_schema_.GetColumn(0).GetName() == name)  //yes_ok!
        {
          std::vector<AbstractExpressionRef> results;
          for (auto pair: expr_map) 
          {
           results.push_back(pair.second);
          }
          return std::make_shared<IndexScanPlanNode>(old_plan.output_schema_, old_plan.table_oid_,index->index_oid_, old_plan.filter_predicate_, results);
        }
      }

    }
  }

  return optimized_plan;
}

}  // namespace bustub

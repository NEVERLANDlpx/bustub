#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"
//add
#include "execution/expressions/logic_expression.h"

namespace bustub {
bool SolveExpression(const AbstractExpressionRef expr, std::vector<AbstractExpressionRef> &vel,std::vector<AbstractExpressionRef> &ver) {
                     
  if (expr == nullptr) 
  {
    return false;
  }
  if (const auto *lexpr = dynamic_cast<const LogicExpression *>(expr.get()); lexpr != nullptr) 
  {
    if (lexpr->logic_type_ == LogicType::And) 
    {
      return SolveExpression(lexpr->children_[0], vel, ver) && SolveExpression(lexpr->children_[1], vel, ver);
    }
  } 
  else if (const auto *cexpr = dynamic_cast<const ComparisonExpression *>(expr.get()); cexpr != nullptr) 
  {
    const auto *l = dynamic_cast<const ColumnValueExpression *>(cexpr->children_[0].get());
    const auto *r = dynamic_cast<const ColumnValueExpression *>(cexpr->children_[1].get());
    if (l != nullptr && r != nullptr) 
    {
      if (l->GetTupleIdx() == 0 && r->GetTupleIdx() == 1) 
      {
        vel.push_back(cexpr->children_[0]);
        ver.push_back(cexpr->children_[1]);
        return true;
      }
      if (r->GetTupleIdx() == 0 && l->GetTupleIdx() == 1) 
      {
        vel.push_back(cexpr->children_[1]);
        ver.push_back(cexpr->children_[0]);
        return true;
      }
    }
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
    std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) 
  {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) 
  {
    const auto &loopplan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> lve;
    std::vector<AbstractExpressionRef> rve;
    if (SolveExpression(loopplan.predicate_, lve, rve)) 
    {
      return std::make_shared<HashJoinPlanNode>(loopplan.output_schema_, loopplan.GetLeftPlan(), loopplan.GetRightPlan(), lve, rve, loopplan.GetJoinType());       
    }
  }

  return optimized_plan;
 // return plan;
}

}  // namespace bustub

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

bool canopt(const AbstractExpressionRef expr, std::vector<AbstractExpressionRef> &left,std::vector<AbstractExpressionRef> &right) {
  if (expr == nullptr) 
  {
    return false;
  }
  auto *logi_expr = dynamic_cast<const LogicExpression *>(expr.get());
  auto *cmp_expr = dynamic_cast<const ComparisonExpression *>(expr.get()); 
  if (logi_expr != nullptr) 
  {
    if (logi_expr->logic_type_ == LogicType::And) 
    {
      return canopt(logi_expr->children_[0], left,right) && canopt(logi_expr->children_[1],left,right);
    }
  } 
  else if (cmp_expr != nullptr) 
  {
    auto *l = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[0].get());
    auto *r = dynamic_cast<const ColumnValueExpression *>(cmp_expr->children_[1].get());
    if (l != nullptr && r != nullptr) 
    {
      if (l->GetTupleIdx() == 0 && r->GetTupleIdx() == 1) 
      {
        left.push_back(cmp_expr->children_[0]);
        right.push_back(cmp_expr->children_[1]);
        return true;
      }
      else if (r->GetTupleIdx() == 0 && l->GetTupleIdx() == 1) 
      {
        left.push_back(cmp_expr->children_[1]);
        right.push_back(cmp_expr->children_[0]);
        return true;
      }
    }
  }
  return false;
}

auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) 
  {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) 
  {
    const auto &old_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    std::vector<AbstractExpressionRef> lexpr;
    std::vector<AbstractExpressionRef> rexpr;
    if (canopt(old_plan.predicate_, lexpr, rexpr)) 
    {
      return std::make_shared<HashJoinPlanNode>(old_plan.output_schema_, old_plan.GetLeftPlan(), old_plan.GetRightPlan(), lexpr, rexpr, old_plan.GetJoinType());
                                               
    }
  }

  return optimized_plan;
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-condistions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
}

}  // namespace bustub

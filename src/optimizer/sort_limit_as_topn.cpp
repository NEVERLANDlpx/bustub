#include "optimizer/optimizer.h"
//add
#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
//add
namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (auto child : plan->GetChildren()) 
  {
    children.emplace_back(OptimizeSortLimitAsTopN(child));//对当前计划节点的每个子节点调用 OptimizeSortLimitAsTopN 函数进行优化，并将优化后的子节点添加到 children 向量
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children)); //使用 CloneWithChildren 函数将原始计划节点与优化后的子节点克隆到 optimized_plan 中
  /*
  首先检查 optimized_plan 的类型是否为限制计划（PlanType::Limit）
  如果是限制计划，它进一步检查该计划的子节点是否为排序计划（PlanType::Sort）
  如果限制计划的子节点是排序计划，它获取排序计划的排序条件，并使用这些条件和限制数创建一个新的顶部N计划节点（TopNPlanNode）
  顶部N计划节点通常用于获取排序后的前N行结果。
  最后，返回新创建的顶部N计划节点
  如果未满足优化条件，则返回原始的优化计划节点 optimized_plan
  */
  if(optimized_plan->GetType()==PlanType::Limit)
  {
  //知道一个基类指针或引用实际上指向的是一个派生类对象时，可以使用 dynamic_cast 来尝试获取该派生类的指针或引用
    auto limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    auto limit = limit_plan.GetLimit();
    if (optimized_plan->GetChildAt(0)->GetType() == PlanType::Sort)  //limit_plan.children_.size() == 1, Limit Plan should have exactly 1 child.
    {
        auto sort_plan_ = dynamic_cast<const SortPlanNode &>(*optimized_plan->GetChildAt(0));
        auto order_by_type = sort_plan_.GetOrderBy();
        return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan_.GetChildAt(0), order_by_type, limit); //sort_plan.children_.size() == 1, "Sort Plan should have exactly 1 child."
    }
  }
    return optimized_plan;
  //return plan;
}

}  // namespace bustub

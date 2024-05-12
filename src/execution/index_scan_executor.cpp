//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_info_(exec_ctx->GetCatalog()->GetTable(plan->table_oid_)),
      index_info_(exec_ctx->GetCatalog()->GetIndex(plan->index_oid_)) {}

void IndexScanExecutor::Init() { 
  for (auto cons_key : plan_->pred_keys_) {
    std::vector<RID> results;

    std::vector<Column> cols;
    std::vector<Value> vals;
    Schema tmpschema(cols);
    Tuple tmptuple;
    auto val = cons_key->Evaluate(&tmptuple, tmpschema);
    cols.push_back(val.GetColumn());
    vals.push_back(val);
    Schema oneschema(cols);
    index_info_->index_->ScanKey(Tuple{vals, &oneschema}, &results, exec_ctx_->GetTransaction());
    for ( int i=0;i<results.size();i++) 
    {
      rids_.push_back(results[i]);
    }
  }
    idx_ = 0; 
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (idx_>= rids_.size()) return false;
    *rid = rids_[idx_];
  *tuple = (table_info_->table_->GetTuple(rids_[idx_])).second;
  idx_++;
  return true;
}

}  // namespace bustub

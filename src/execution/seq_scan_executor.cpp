//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      table_metadata_(exec_ctx->GetCatalog()->GetTable(plan->GetTableOid())),
      table_iterator_(table_metadata_->table_->Begin(exec_ctx->GetTransaction())) {}

void SeqScanExecutor::Init() {}

bool SeqScanExecutor::Next(Tuple *tuple) {
  auto predicate = plan_->GetPredicate();
  while (table_iterator_ != table_metadata_->table_->End()) {
    *tuple = *table_iterator_++;
    if (!predicate || predicate->Evaluate(tuple, &table_metadata_->schema_).GetAs<bool>()) {
      return true;
    }
  }

  return false;
}

}  // namespace bustub

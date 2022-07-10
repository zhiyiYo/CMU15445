//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/insert_executor.h"

#include <memory>

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_executor_(std::move(child_executor)),
      table_metadata_(exec_ctx->GetCatalog()->GetTable(plan->TableOid())) {}

const Schema *InsertExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void InsertExecutor::Init() {}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple) {
  RID rid;

  if (plan_->IsRawInsert()) {
    for (const auto &values : plan_->RawValues()) {
      Tuple tuple(values, &table_metadata_->schema_);
      if (!table_metadata_->table_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction())) {
        return false;
      };
    }
  } else {
    Tuple tuple;
    while (child_executor_->Next(&tuple)) {
      if (!table_metadata_->table_->InsertTuple(tuple, &rid, exec_ctx_->GetTransaction())) {
        return false;
      };
    }
  }

  return true;
}

}  // namespace bustub

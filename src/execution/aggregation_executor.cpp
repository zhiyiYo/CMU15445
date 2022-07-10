//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/aggregation_executor.h"

#include <memory>
#include <vector>

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

const Schema *AggregationExecutor::GetOutputSchema() { return plan_->OutputSchema(); }

void AggregationExecutor::Init() {
  child_->Init();

  // initialize aggregation hash table
  Tuple tuple;
  while (child_->Next(&tuple)) {
    aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
  }

  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple) {
  auto having = plan_->GetHaving();
  auto out_schema = GetOutputSchema();

  while (aht_iterator_ != aht_.End()) {
    auto group_bys = aht_iterator_.Key().group_bys_;
    auto aggregates = aht_iterator_.Val().aggregates_;

    if (!having || having->EvaluateAggregate(group_bys, aggregates).GetAs<bool>()) {
      std::vector<Value> values;
      for (uint32_t i = 0; i < out_schema->GetColumnCount(); ++i) {
        auto expr = out_schema->GetColumn(i).GetExpr();
        values.push_back(expr->EvaluateAggregate(group_bys, aggregates));
      }

      *tuple = Tuple(values, out_schema);
      ++aht_iterator_;
      return true;
    }

    ++aht_iterator_;
  }

  return false;
}

}  // namespace bustub

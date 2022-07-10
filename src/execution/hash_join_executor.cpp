//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/hash_join_executor.h"

#include <memory>
#include <vector>

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left, std::unique_ptr<AbstractExecutor> &&right)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left)),
      right_executor_(std::move(right)),
      jht_("join hash table", exec_ctx->GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_) {}

void HashJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();

  // create hash table for left child
  Tuple tuple;
  while (left_executor_->Next(&tuple)) {
    auto h = HashValues(&tuple, left_executor_->GetOutputSchema(), plan_->GetLeftKeys());
    jht_.Insert(exec_ctx_->GetTransaction(), h, tuple);
  }
}

bool HashJoinExecutor::Next(Tuple *tuple) {
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  auto out_schema = GetOutputSchema();
  Tuple right_tuple;

  while (right_executor_->Next(&right_tuple)) {
    // get all tuples with the same hash values in left child
    auto h = HashValues(&right_tuple, right_executor_->GetOutputSchema(), plan_->GetRightKeys());
    std::vector<Tuple> left_tuples;
    jht_.GetValue(exec_ctx_->GetTransaction(), h, &left_tuples);

    // get the exact matching left tuple
    for (auto &left_tuple : left_tuples) {
      if (plan_->Predicate()->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
        // create output tuple
        std::vector<Value> values;
        for (uint32_t i = 0; i < out_schema->GetColumnCount(); ++i) {
          auto expr = out_schema->GetColumn(i).GetExpr();
          values.push_back(expr->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema));
        }

        *tuple = Tuple(values, out_schema);
        return true;
      }
    }
  }

  return false;
}
}  // namespace bustub

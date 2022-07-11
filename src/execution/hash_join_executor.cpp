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

  // create temp tuple page
  auto buffer_pool_manager = exec_ctx_->GetBufferPoolManager();
  page_id_t tmp_page_id;
  auto tmp_page = reinterpret_cast<TmpTuplePage *>(buffer_pool_manager->NewPage(&tmp_page_id)->GetData());
  tmp_page->Init(tmp_page_id, PAGE_SIZE);

  // create hash table for left child
  Tuple tuple;
  TmpTuple tmp_tuple(tmp_page_id, 0);
  while (left_executor_->Next(&tuple)) {
    auto h = HashValues(&tuple, left_executor_->GetOutputSchema(), plan_->GetLeftKeys());

    // insert tuple to page, creata a new temp tuple page if page if full
    if (!tmp_page->Insert(tuple, &tmp_tuple)) {
      buffer_pool_manager->UnpinPage(tmp_page_id, true);
      tmp_page = reinterpret_cast<TmpTuplePage *>(buffer_pool_manager->NewPage(&tmp_page_id)->GetData());
      tmp_page->Init(tmp_page_id, PAGE_SIZE);

      // try inserting tuple to page again
      tmp_page->Insert(tuple, &tmp_tuple);
    }

    jht_.Insert(exec_ctx_->GetTransaction(), h, tmp_tuple);
  }

  buffer_pool_manager->UnpinPage(tmp_page_id, true);
}

bool HashJoinExecutor::Next(Tuple *tuple) {
  auto buffer_pool_manager = exec_ctx_->GetBufferPoolManager();
  auto left_schema = left_executor_->GetOutputSchema();
  auto right_schema = right_executor_->GetOutputSchema();
  auto predicate = plan_->Predicate();
  auto out_schema = GetOutputSchema();
  Tuple right_tuple;

  while (right_executor_->Next(&right_tuple)) {
    // get all tuples with the same hash values in left child
    auto h = HashValues(&right_tuple, right_executor_->GetOutputSchema(), plan_->GetRightKeys());
    std::vector<TmpTuple> tmp_tuples;
    jht_.GetValue(exec_ctx_->GetTransaction(), h, &tmp_tuples);

    // get the exact matching left tuple
    for (auto &tmp_tuple : tmp_tuples) {
      // convert tmp tuple to left tuple
      auto page_id = tmp_tuple.GetPageId();
      auto tmp_page = buffer_pool_manager->FetchPage(page_id);
      Tuple left_tuple;
      left_tuple.DeserializeFrom(tmp_page->GetData() + tmp_tuple.GetOffset());
      buffer_pool_manager->UnpinPage(page_id, false);

      if (!predicate || predicate->EvaluateJoin(&left_tuple, left_schema, &right_tuple, right_schema).GetAs<bool>()) {
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

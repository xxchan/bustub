//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.h
//
// Identification: src/include/execution/executors/seq_scan_executor.h
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/seq_scan_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * SeqScanExecutor executes a sequential scan over a table.
 */
class SeqScanExecutor : public AbstractExecutor {
 public:
  /**
   * Creates a new sequential scan executor.
   * @param exec_ctx the executor context
   * @param plan the sequential scan plan to be executed
   */
  SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
      : AbstractExecutor(exec_ctx),
        plan_{plan},
        table_{exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())},
        iter{table_->table_->Begin(exec_ctx_->GetTransaction())},
        iter_end{table_->table_->End()} {}

  void Init() override {}

  bool Next(Tuple *tuple) override {
    auto predicate_ = plan_->GetPredicate();
    for (; iter != iter_end; ++iter) {
      if (predicate_ == nullptr || predicate_->Evaluate(&*iter, GetOutputSchema()).GetAs<bool>()) {
        *tuple = *iter;
        ++iter;
        return true;
      }
    }
    return false;
  }

  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

 private:
  /** The sequential scan plan node to be executed. */
  const SeqScanPlanNode *plan_;
  const TableMetadata *table_;
  TableIterator iter;
  const TableIterator iter_end;
};  // namespace bustub
}  // namespace bustub

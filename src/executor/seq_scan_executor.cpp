//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      iterator_(nullptr, RowId(INVALID_PAGE_ID, 0), nullptr),
      is_schema_same_(false) {}

bool SeqScanExecutor::SchemaEqual(const Schema *table_schema, const Schema *output_schema) {
  auto table_columns = table_schema->GetColumns();
  auto output_columns = output_schema->GetColumns();
  if (table_columns.size() != output_columns.size()) {
    return false;
  }
  int col_size = table_columns.size();
  for (int i = 0; i < col_size; i++) {
    if ((table_columns[i]->GetName() != output_columns[i]->GetName()) ||
        (table_columns[i]->GetType() != output_columns[i]->GetType()) ||
        (table_columns[i]->GetLength() != output_columns[i]->GetLength())) {
      return false;
    }
  }
  return true;
}

void SeqScanExecutor::TupleTransfer(const Schema *table_schema, const Schema *output_schema, const Row *row,
                                    Row *output_row) {
  const auto &output_columns = output_schema->GetColumns();
  std::vector<Field> dest_row;
  dest_row.reserve(output_columns.size());
  for (const auto column : output_columns) {
    auto idx = column->GetTableInd();
    dest_row.emplace_back(*row->GetField(idx));
  }
  *output_row = Row(dest_row);
}

void SeqScanExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  auto first_row = table_info_->GetTableHeap()->Begin(nullptr);
  iterator_ = (table_info_->GetTableHeap()->Begin(exec_ctx_->GetTransaction()));
  schema_ = plan_->OutputSchema();
  is_schema_same_ = SchemaEqual(table_info_->GetSchema(), schema_);
}

bool SeqScanExecutor::Next(Row *row, RowId *rid) {
  auto predicate = plan_->GetPredicate();
  auto table_schema = table_info_->GetSchema();
  while (iterator_ != table_info_->GetTableHeap()->End()) {
    auto p_row = &(*iterator_);
    if (predicate != nullptr) {
      if (!predicate->Evaluate(p_row).CompareEquals(Field(kTypeInt, 1))) {
        iterator_++;
        continue;
      }
    }
    *rid = iterator_->GetRowId();
    if (!is_schema_same_) {
      TupleTransfer(table_schema, schema_, p_row, row);
    } else {
      *row = *p_row;
    }
    iterator_++;
    return true;
  }
  return false;
}

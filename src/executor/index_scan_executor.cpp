#include "executor/executors/index_scan_executor.h"

class RowidCompare {
 public:
  bool operator()(RowId rid1, RowId rid2) { return rid1.Get() < rid2.Get(); }
};

IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init() {
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  auto first_row = table_info_->GetTableHeap()->Begin(nullptr);
  result_ = IndexScan(plan_->GetPredicate());
  is_schema_same_ = SchemaEqual(table_info_->GetSchema(), plan_->OutputSchema());
}

bool IndexScanExecutor::SchemaEqual(const Schema *table_schema, const Schema *output_schema) {
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

void IndexScanExecutor::TupleTransfer(const Schema *table_schema, const Schema *output_schema, const Row *row,
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

vector<RowId> IndexScanExecutor::IndexScan(AbstractExpressionRef predicate) {
  switch (predicate->GetType()) {
    case ExpressionType::LogicExpression: {
      vector<RowId> lhs = IndexScan(predicate->GetChildAt(0));
      vector<RowId> rhs = IndexScan(predicate->GetChildAt(1));
      // The function cannot differentiate between the case where the number of rows that
      // meet the criteria in the indexed column is zero and the case where the column is
      // not indexed. Writing it this way will ignore the filtering effect brought about
      // by having zero rows that meet the criteria in the indexed column.
      if (plan_->need_filter_) {
        if (lhs.empty()) return rhs;
        if (rhs.empty()) return lhs;
      }
      sort(lhs.begin(), lhs.end(), RowidCompare());
      sort(rhs.begin(), rhs.end(), RowidCompare());
      vector<RowId> result;
      set_intersection(lhs.begin(), lhs.end(), rhs.begin(), rhs.end(), back_inserter(result), RowidCompare());
      return result;
    }
    case ExpressionType::ComparisonExpression: {
      std::vector<RowId> ret;
      std::vector<Field> fields{predicate->GetChildAt(1)->Evaluate(nullptr)};
      Row key(fields);
      for (auto index : plan_->indexes_) {
        uint32_t col_idx = dynamic_pointer_cast<ColumnValueExpression>(predicate->GetChildAt(0))->GetColIdx();
        if (col_idx == index->GetIndexKeySchema()->GetColumn(0)->GetTableInd()) {
          index->GetIndex()->ScanKey(key, ret, nullptr,
                                     dynamic_pointer_cast<ComparisonExpression>(predicate)->GetComparisonType());
          break;
        }
      }
      return ret;
    }
    default:
      break;
  }
}

bool IndexScanExecutor::Next(Row *row, RowId *rid) {
  auto predicate = plan_->GetPredicate();
  auto table_schema = table_info_->GetSchema();
  while (cursor_ < result_.size()) {
    auto p_row = new Row(result_[cursor_]);
    table_info_->GetTableHeap()->GetTuple(p_row, nullptr);
    if (plan_->need_filter_) {
      if (!predicate->Evaluate(p_row).CompareEquals(Field(kTypeInt, 1))) {
        cursor_++;
        delete p_row;
        continue;
      }
    }
    *rid = result_[cursor_];
    if (!is_schema_same_) {
      TupleTransfer(table_schema, plan_->OutputSchema(), p_row, row);
    } else {
      *row = *p_row;
    }
    delete p_row;
    cursor_++;
    return true;
  }
  return false;
}

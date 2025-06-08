//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  schema_ = table_info_->GetSchema();
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), index_info_);
}

bool InsertExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
    Row insert_row;
    RowId insert_rid;
    if (child_executor_->Next(&insert_row, &insert_rid)) {
        for (auto info: index_info_) {
            Row key_row;
            insert_row.GetKeyFromRow(table_info_->GetSchema(), info->GetIndexKeySchema(), key_row);
            std::vector<RowId> result;
            if (!key_row.GetFields().empty() &&
                info->GetIndex()->ScanKey(key_row, result, exec_ctx_->GetTransaction()) == DB_SUCCESS) {
                std::cout << "key already exists" << std::endl;
                return false;
            }
        }
        if (table_info_->GetTableHeap()->InsertTuple(insert_row, exec_ctx_->GetTransaction())) {
            Row key_row;
            for (auto info: index_info_) {  // 更新索引
                insert_row.GetKeyFromRow(schema_, info->GetIndexKeySchema(), key_row);
                info->GetIndex()->InsertEntry(key_row, insert_row.GetRowId(), exec_ctx_->GetTransaction());
            }
            return true;
        }
  }
  return false;
}
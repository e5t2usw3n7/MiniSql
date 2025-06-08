//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info_);
  exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->GetTableName(), index_info_);
  txn_ = exec_ctx_->GetTransaction();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *row, RowId *rid) {
  if (child_executor_->Next(row, rid)) {
    if (!table_info_->GetTableHeap()->MarkDelete(*rid, txn_)) {
      return false;
    }
    Row key_row;
    for (auto info : index_info_) {  // 更新索引
      row->GetKeyFromRow(table_info_->GetSchema(), info->GetIndexKeySchema(), key_row);
      info->GetIndex()->RemoveEntry(key_row, *rid, txn_);
    }
    return true;
  }
  return false;
}
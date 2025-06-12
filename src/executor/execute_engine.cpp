#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

static bool supress_output = false;
ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }

  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
  if (current_db_.empty()) {
    return DB_FAILED;
  }

  // 提取表名
  if (ast->child_->type_ != kNodeIdentifier) {
    return DB_FAILED;
  }
  std::string new_table_name(ast->child_->val_);

  // 提取列定义列表
  std::vector<Column *> col_defs;
  uint32_t col_idx = 0;
  auto col_node_list = ast->child_->next_;
  if (col_node_list->type_ != kNodeColumnDefinitionList) {
    return DB_FAILED;
  }

  // 解析主键
  std::vector<std::string> primary_keys;
  std::vector<std::string> unique_keys;
  auto cur_def = col_node_list->child_;
  auto collist_node = cur_def;
  while (collist_node != nullptr && collist_node->type_ != kNodeColumnList) {
    collist_node = collist_node->next_;
  }
  if (collist_node && collist_node->type_ == kNodeColumnList) {
    auto pk_node = collist_node->child_;
    while (pk_node && pk_node->type_ == kNodeIdentifier) {
      primary_keys.push_back(pk_node->val_);
      pk_node = pk_node->next_;
    }
  }

  // 逐列解析字段定义
  while (cur_def && cur_def->type_ == kNodeColumnDefinition) {
    Column *col_ptr = nullptr;
    std::string col_name = cur_def->child_->val_;
    std::string col_type = cur_def->child_->next_->val_;
    bool col_unique = false;

    if (cur_def->val_ != nullptr && std::string(cur_def->val_) == "unique") {
      col_unique = true;
      unique_keys.push_back(col_name);
    }

    if (std::find(primary_keys.begin(), primary_keys.end(), col_name) != primary_keys.end()) {
      col_unique = true;
      unique_keys.push_back(col_name);
    }

    if (col_type == "int") {
      col_ptr = new Column(col_name, kTypeInt, col_idx, true, col_unique);
    } else if (col_type == "float") {
      col_ptr = new Column(col_name, kTypeFloat, col_idx, true, col_unique);
    } else if (col_type == "char") {
      uint32_t len = std::stoi(cur_def->child_->next_->child_->val_);
      col_ptr = new Column(col_name, kTypeChar, len, col_idx, true, col_unique);
    }

    col_defs.push_back(col_ptr);
    col_idx++;
    cur_def = cur_def->next_;
  }

  // 创建表
  auto *catalog_mgr = context->GetCatalog();
  Schema *schema_obj = new Schema(col_defs);
  TableInfo *tbl_info = nullptr;
  auto status = catalog_mgr->CreateTable(new_table_name, schema_obj, context->GetTransaction(), tbl_info);
  if (status != DB_SUCCESS) {
    return status;
  }

  // 创建唯一索引
  for (const auto &u_col : unique_keys) {
    std::string idx_name = "UNIQUE_" + u_col + "_ON_" + new_table_name;
    IndexInfo *idx_info = nullptr;
    std::vector<std::string> idx_col = {u_col};
    catalog_mgr->CreateIndex(new_table_name, idx_name, idx_col, context->GetTransaction(), idx_info, "btree");
  }

  // 创建主键索引（自动）
  if (!primary_keys.empty()) {
    std::string auto_idx_name = "AUTO_CREATED_INDEX_OF_";
    for (const auto &pk : primary_keys) {
      auto_idx_name += pk + "_";
    }
    auto_idx_name += "ON_" + new_table_name;
    IndexInfo *auto_idx = nullptr;
    catalog_mgr->CreateIndex(new_table_name, auto_idx_name, primary_keys, context->GetTransaction(), auto_idx, "btree");
  }

  return status;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
  // 确保当前已选择数据库
  if (current_db_.empty()) {
    return DB_FAILED;
  }

  // 获取 CatalogManager 指针
  auto *catalog_mgr = context->GetCatalog();

  // 提取表名
  std::string drop_table_name(ast->child_->val_);

  // 先尝试删除表
  dberr_t drop_result = catalog_mgr->DropTable(drop_table_name);
  if (drop_result != DB_SUCCESS) {
    return drop_result;
  }

  // 然后查找该表下所有索引
  std::vector<IndexInfo *> related_indexes;
  catalog_mgr->GetTableIndexes(drop_table_name, related_indexes);

  // 逐个删除索引
  for (auto *index_ptr : related_indexes) {
    std::string idx_name = index_ptr->GetIndexName();
    catalog_mgr->DropIndex(drop_table_name, idx_name);
  }

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
  // 当前未选择数据库，直接失败
  if (current_db_.empty()) {
    return DB_FAILED;
  }

  // 获取目录管理器
  CatalogManager *cat_mgr = context->GetCatalog();

  // 获取所有表的信息
  std::vector<TableInfo *> all_tables;
  cat_mgr->GetTables(all_tables);

  // 遍历所有表并列出其索引
  int index_total = 0;
  std::cout << "[Index Summary]" << std::endl;

  for (TableInfo *tbl_ptr : all_tables) {
    std::vector<IndexInfo *> tbl_indexes;
    std::string tbl_name = tbl_ptr->GetTableName();

    // 获取当前表的所有索引
    cat_mgr->GetTableIndexes(tbl_name, tbl_indexes);

    std::cout << " - Table: " << tbl_name << std::endl;
    for (IndexInfo *idx : tbl_indexes) {
      std::string idx_name = idx->GetIndexName();
      std::cout << "    > Index: " << idx_name << std::endl;
      index_total++;
    }
  }

  std::cout << "Total: " << index_total << " index(es) displayed." << std::endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
  // 数据库未选择，无法执行建索引操作
  if (current_db_.empty()) {
    return DB_FAILED;
  }

  // 提取索引名和目标表名
  std::string index_id = ast->child_->val_;
  std::string target_tbl = ast->child_->next_->val_;

  // 获取列名节点
  pSyntaxNode col_list_node = ast->child_->next_->next_;
  if (col_list_node == nullptr || col_list_node->type_ != kNodeColumnList) {
    return DB_FAILED;
  }

  // 解析列名集合
  std::vector<std::string> index_columns;
  pSyntaxNode col_ptr = col_list_node->child_;
  while (col_ptr != nullptr) {
    std::string column_name = col_ptr->val_;
    index_columns.emplace_back(column_name);
    col_ptr = col_ptr->next_;
  }

  // 调用 Catalog 创建索引
  CatalogManager *cat_mgr = context->GetCatalog();
  IndexInfo *created_index_ptr = nullptr;
  dberr_t create_result = cat_mgr->CreateIndex(
      target_tbl, index_id, index_columns, context->GetTransaction(), created_index_ptr, "btree");

  return create_result;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
  // 若未选定数据库，无法进行索引删除
  if (current_db_.empty()) {
    return DB_FAILED;
  }

  // 获取 Catalog 管理器
  CatalogManager *cat_mgr = context->GetCatalog();

  // 获取索引名称
  std::string target_index = ast->child_->val_;

  // 遍历查找包含该索引的表名
  auto &index_map = cat_mgr->index_names_;
  auto iter = index_map.begin();
  while (iter != index_map.end()) {
    const std::string &tbl_name = iter->first;
    const auto &idx_map = iter->second;
    if (idx_map.find(target_index) != idx_map.end()) {
      break;
    }
    ++iter;
  }

  // 若找不到索引，返回错误
  if (iter == index_map.end()) {
    return DB_INDEX_NOT_FOUND;
  }

  // 执行索引删除操作
  std::string source_table = iter->first;
  dberr_t drop_status = cat_mgr->DropIndex(source_table, target_index);
  return drop_status;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
extern "C" {
  int yyparse(void);
#include <parser/minisql_lex.h>
#include <parser/parser.h>
}

dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
  std::string path = ast->child_->val_;
  FILE *input = fopen(path.c_str(), "r");
  if (input == nullptr) {
    return DB_FAILED;
  }

  const int kMaxBufferSize = 1024;
  char sql[kMaxBufferSize];

  std::cout << "Execfile started, output supressed." << std::endl;
  auto begin = std::chrono::system_clock::now();
  supress_output = true;

  while (!feof(input)) {
    memset(sql, 0, sizeof(sql));
    int pos = 0;
    char ch;

    while (!feof(input) && (ch = getc(input)) != ';') {
      sql[pos++] = ch;
    }

    if (feof(input)) continue;
    sql[pos] = ch;

    YY_BUFFER_STATE buffer_state = yy_scan_string(sql);
    if (buffer_state == nullptr) {
      fprintf(stderr, "yy_scan_string failed\n");
      exit(1);
    }
    yy_switch_to_buffer(buffer_state);

    MinisqlParserInit();
    yyparse();

    if (MinisqlParserGetError()) {
      printf("%s\n", MinisqlParserGetErrorMessage());
    } else {
      pSyntaxNode root = MinisqlGetParserRootNode();
      dberr_t exec_result = Execute(root);
      ExecuteInformation(exec_result);
    }

    MinisqlParserFinish();
    yy_delete_buffer(buffer_state);
    yylex_destroy();
  }

  supress_output = false;
  auto end = std::chrono::system_clock::now();
  double duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - begin).count();
  std::cout << "Execfile finished in " << duration << " ms" << std::endl;

  fclose(input);
  return DB_SUCCESS;
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
  current_db_ = "";
  return DB_QUIT;
}

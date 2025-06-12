#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  buf += 4;
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
/////
uint32_t CatalogMeta::GetSerializedSize() const {
    uint32_t size = sizeof(CATALOG_METADATA_MAGIC_NUM) + sizeof(uint32_t) * 2; // magic num + 2 uint32_t for table and index counts
    size += table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t)); // each table entry
    size += index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t)); // each index entry
    return size;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (!init) {
    // 获取元数据页面
    Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(meta_page->GetData()));

    // 获取当前的 next_table_id_ 和 next_index_id_
    next_table_id_ = std::max(static_cast<unsigned int>(next_table_id_.load()), catalog_meta_->GetNextTableId());
    next_index_id_ = std::max(static_cast<unsigned int>(next_index_id_.load()), catalog_meta_->GetNextIndexId());

    // 加载表的元数据
    for (const auto &entry : catalog_meta_->table_meta_pages_) {
      page_id_t table_meta_pid = entry.second;
      Page *table_meta_page = buffer_pool_manager_->FetchPage(table_meta_pid);
      TableMetadata *table_meta = nullptr;
      TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta);
      buffer_pool_manager_->UnpinPage(table_meta_pid, true);

      // 获取表信息并保存
      table_id_t tbl_id = table_meta->GetTableId();
      std::string tbl_name = table_meta->GetTableName();
      table_names_[tbl_name] = tbl_id;

      // 创建表堆
      TableHeap *tbl_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(),
                                               table_meta->GetSchema(), log_manager_, lock_manager_);

      // 创建表信息对象
      TableInfo *tbl_info = TableInfo::Create();
      tbl_info->Init(table_meta, tbl_heap);
      tables_[tbl_id] = tbl_info;

      // 更新 next_table_id_
      next_table_id_ = std::max(next_table_id_.load(), tbl_id + 1);
    }

    // 加载索引的元数据
    for (const auto &entry : catalog_meta_->index_meta_pages_) {
      page_id_t index_meta_pid = entry.second;
      Page *index_meta_page = buffer_pool_manager_->FetchPage(index_meta_pid);
      IndexMetadata *index_meta = nullptr;
      IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta);
      buffer_pool_manager_->UnpinPage(index_meta_pid, true);

      // 获取索引信息并保存
      index_id_t idx_id = index_meta->GetIndexId();
      table_id_t tid = index_meta->GetTableId();
      std::string table_name = tables_[tid]->GetTableName();
      std::string index_name = index_meta->GetIndexName();

      index_names_[table_name][index_name] = idx_id;

      // 创建索引信息对象
      IndexInfo *idx_info = IndexInfo::Create();
      idx_info->Init(index_meta, tables_[tid], buffer_pool_manager_);
      indexes_[idx_id] = idx_info;

      // 更新 next_index_id_
      next_index_id_ = std::max(next_index_id_.load(), idx_id + 1);
    }

  } else {
    catalog_meta_ = CatalogMeta::NewInstance();
  }

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
//////
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // 检查表是否已存在
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }

  // 使用 Schema 的拷贝构造函数进行深拷贝
  std::vector<Column *> columns = schema->GetColumns();  // 获取列的列表
  TableSchema *copied_schema = new TableSchema(columns, true);  // 通过列和默认的 is_manage_ 参数构造表结构

  // 获取新表 ID，并将表名与表 ID 进行映射
  table_id_t current_table_id = next_table_id_;
  table_names_[table_name] = current_table_id;

  // 创建新的页来存放表的元数据
  page_id_t meta_page_id;
  Page *meta_page = buffer_pool_manager_->NewPage(meta_page_id);

  // 创建并初始化表堆
  TableHeap *heap = TableHeap::Create(buffer_pool_manager_, copied_schema, txn, log_manager_, lock_manager_);

  // 创建表的元数据对象并将其序列化
  TableMetadata *table_metadata = TableMetadata::Create(current_table_id, table_name, heap->GetFirstPageId(), copied_schema);
  table_metadata->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(meta_page_id, true);

  // 记录表的元数据页 ID
  catalog_meta_->table_meta_pages_[current_table_id] = meta_page_id;

  // 创建表信息对象并进行初始化
  table_info = TableInfo::Create();
  table_info->Init(table_metadata, heap);
  tables_[current_table_id] = table_info;

  // 更新下一个表的 ID
  next_table_id_++;

  // 持久化表的元数据
  FlushCatalogMetaPage();  // 持久化更新后的表元数据

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
    if (table_names_.find(table_name) != table_names_.end()) {
        table_id_t table_id = table_names_[table_name];
        table_info = tables_[table_id];
        return DB_SUCCESS;
    }
    return DB_TABLE_NOT_EXIST;  
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
    // 使用迭代器进行遍历，避免使用范围基 for 循环
    for (std::unordered_map<table_id_t, TableInfo*>::const_iterator it = tables_.begin(); it != tables_.end(); ++it) {
        tables.push_back(it->second);
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
    // 获取表的信息
    TableInfo *table_info = nullptr;
    dberr_t ret = GetTable(table_name, table_info);
    if (ret != DB_SUCCESS) {
        return DB_TABLE_NOT_EXIST;  // 如果表不存在，返回错误
    }

    // 验证索引列是否存在于表中
    for (const auto &key : index_keys) {
        uint32_t col_index;
        bool is_valid_column = (table_info->GetSchema()->GetColumnIndex(key, col_index) == DB_SUCCESS);
        if (!is_valid_column) {
            return DB_COLUMN_NAME_NOT_EXIST;  // 如果某列不存在，返回错误
        }
    }

    // 检查该索引是否已经存在
    if (index_names_[table_name].count(index_name)) {
        return DB_INDEX_ALREADY_EXIST;  // 如果索引已经存在，返回错误
    }

    // 获取下一个索引 ID
    index_id_t index_id = next_index_id_++;

    // 创建并填充索引元数据
    std::vector<unsigned int> col_indexes;
    TableSchema *table_schema = table_info->GetSchema();
    for (const auto &key : index_keys) {
        unsigned int col_idx;
        if (table_schema->GetColumnIndex(key, col_idx) != DB_SUCCESS) {
            return DB_COLUMN_NAME_NOT_EXIST;  // 返回错误
        }
        col_indexes.push_back(col_idx);
    }

    IndexMetadata *index_metadata = IndexMetadata::Create(index_id, index_name, table_info->GetTableId(), col_indexes);

    // 创建索引信息对象
    index_info = IndexInfo::Create();
    index_info->Init(index_metadata, table_info, buffer_pool_manager_);

    // 将索引添加到目录中
    indexes_[index_id] = index_info;
    index_names_[table_name][index_name] = index_id;

    // 持久化索引元数据
    FlushCatalogMetaPage();  // 刷新并持久化修改

    return DB_SUCCESS;
}
 
/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
    // 查找表名是否存在
    auto table_iter = index_names_.find(table_name);
    if (table_iter == index_names_.end()) {
        return DB_TABLE_NOT_EXIST;  // 表不存在，返回错误
    }

    // 查找索引名是否存在
    auto index_iter = table_iter->second.find(index_name);
    if (index_iter == table_iter->second.end()) {
        return DB_INDEX_NOT_FOUND;  // 索引不存在，返回错误
    }

    // 获取索引 ID 并找到对应的索引信息
    index_id_t index_id = index_iter->second;
    index_info = indexes_.count(index_id) > 0 ? indexes_.at(index_id) : nullptr;

    // 如果没有找到索引信息，返回错误
    if (index_info == nullptr) {
        return DB_INDEX_NOT_FOUND;
    }

    return DB_SUCCESS;  // 成功找到索引
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
    auto table_iter = index_names_.find(table_name);
    if (table_iter == index_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    for (const auto &index_entry : table_iter->second) {
        auto index_iter = indexes_.find(index_entry.second);
        if (index_iter != indexes_.end()) {
            indexes.push_back(index_iter->second);
        }
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::DropTable(const string &table_name) {
    TableInfo *table_info = nullptr;
    dberr_t result = GetTable(table_name, table_info);
    if (result != DB_SUCCESS) {
        return result;
    }

    table_names_.erase(table_name);
    tables_.erase(table_info->GetTableId());

    // Also drop any indexes associated with this table
    auto index_iter = index_names_.find(table_name);
    if (index_iter != index_names_.end()) {
        for (const auto &index : index_iter->second) {
            DropIndex(table_name, index.first);
        }
        index_names_.erase(index_iter);
    }

    // Remove table meta info
    catalog_meta_->GetTableMetaPages()->erase(table_info->GetTableId());
    FlushCatalogMetaPage();

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
    // 查找对应表的索引映射
    auto tbl_idx_iter = index_names_.find(table_name);
    if (tbl_idx_iter == index_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

    // 查找具体索引名
    auto &idx_map = tbl_idx_iter->second;
    auto idx_iter = idx_map.find(index_name);
    if (idx_iter == idx_map.end()) {
        return DB_INDEX_NOT_FOUND;
    }

    index_id_t idx_id = idx_iter->second;

    // 从 index_names 和 indexes 中删除
    idx_map.erase(index_name);
    if (idx_map.empty()) {
        index_names_.erase(table_name);
    }

    // 从 indexes 中删除
    indexes_.erase(idx_id);  // 使用索引ID直接删除

    // 删除对应的元数据页
    page_id_t meta_pid = catalog_meta_->index_meta_pages_[idx_id];
    catalog_meta_->index_meta_pages_.erase(idx_id);

    // 确保页面被释放并删除
    buffer_pool_manager_->UnpinPage(meta_pid, true);
    buffer_pool_manager_->DeletePage(meta_pid);

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::FlushCatalogMetaPage() const {
    // 使用缓冲池管理器获取元数据页面
    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);

    // 将当前 catalog_meta_ 的数据序列化到页面
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());

    // 确保页面标记为脏并且被刷新到磁盘
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);  // 标记页面为脏
    buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);  // 强制刷新页面到磁盘

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
    // 如果该表已经加载过，直接返回
    if (tables_.find(table_id) != tables_.end()) {
        return DB_TABLE_ALREADY_EXIST;
    }

    // 注册表的元数据页信息
    catalog_meta_->table_meta_pages_[table_id] = page_id;

    // 从缓冲池读取表元数据页
    Page *meta_page = buffer_pool_manager_->FetchPage(page_id);
    char *meta_data = reinterpret_cast<char *>(meta_page->GetData());

    // 反序列化表元数据
    TableMetadata *table_metadata = nullptr;
    TableMetadata::DeserializeFrom(meta_data, table_metadata);

    // 记录表名与表 ID 映射
    std::string table_name = table_metadata->GetTableName();
    table_names_[table_name] = table_id;

    // 创建表堆并初始化表信息对象
    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, page_id, table_metadata->GetSchema(),
                                              log_manager_, lock_manager_);
    TableInfo *table_info = TableInfo::Create();
    table_info->Init(table_metadata, table_heap);

    // 注册表信息
    tables_[table_id] = table_info;

    // 解除页面锁定
    buffer_pool_manager_->UnpinPage(page_id, true);

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
    // 如果索引已经存在，返回错误
    if (indexes_.find(index_id) != indexes_.end()) {
        return DB_INDEX_ALREADY_EXIST;
    }

    // 更新索引元数据页面
    catalog_meta_->index_meta_pages_[index_id] = page_id;

    // 使用缓冲池读取页面数据
    Page *index_page = buffer_pool_manager_->FetchPage(page_id);
    char *page_data = reinterpret_cast<char *>(index_page->GetData());

    // 反序列化索引元数据
    IndexMetadata *index_metadata = nullptr;
    IndexMetadata::DeserializeFrom(page_data, index_metadata);

    // 获取关联的表信息
    auto associated_table_info = tables_[index_metadata->GetTableId()];
    std::string associated_table_name = associated_table_info->GetTableName();
    
    // 将索引名称映射到索引 ID
    index_names_[associated_table_name][index_metadata->GetIndexName()] = index_id;

    // 创建新的索引信息对象
    IndexInfo *new_index_info = IndexInfo::Create();
    new_index_info->Init(index_metadata, associated_table_info, buffer_pool_manager_);

    // 将索引信息添加到索引容器中
    indexes_[index_id] = new_index_info;

    // 解除页面锁定
    buffer_pool_manager_->UnpinPage(page_id, true);

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
/////
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        table_info = it->second;
        return DB_SUCCESS;
    }
    return DB_TABLE_NOT_EXIST;
}
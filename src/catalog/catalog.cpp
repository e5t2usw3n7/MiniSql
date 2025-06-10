#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
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
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
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
//    ASSERT(false, "Not Implemented yet");
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
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
    table_id_t table_id = next_table_id_++;

    TableSchema *copied_schema = Schema::DeepCopySchema(schema);  // 使用 DeepCopySchema 方法

    TableMetadata *table_metadata = TableMetadata::Create(table_id, table_name, 0, copied_schema); // Root page ID 设置为 0
    table_info = TableInfo::Create();
    table_info->Init(table_metadata, nullptr);  // TableHeap 后续初始化

    tables_[table_id] = table_info;
    table_names_[table_name] = table_id;

    // 持久化元数据
    FlushCatalogMetaPage();  // 修改为正确的刷新方法

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
    if (table_names_.find(table_name) != table_names_.end()) {
        table_id_t table_id = table_names_[table_name];
        table_info = tables_[table_id];
        return DB_SUCCESS;
    }
    return DB_TABLE_NOT_EXIST;  // 修改为 DB_TABLE_NOT_EXIST
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
    // 获取表信息
    TableInfo *table_info = nullptr;
    dberr_t ret = GetTable(table_name, table_info);
    if (ret != DB_SUCCESS) {
        return DB_TABLE_NOT_EXIST;  // 如果表不存在，返回错误
    }

    // 检查索引键是否有效
    for (const auto &key : index_keys) {
        bool column_exists = false;
        for (const auto &col : table_info->GetSchema()->GetColumns()) {
            if (col->GetName() == key) {
                column_exists = true;
                break;
            }
        }
        if (!column_exists) {
            return DB_COLUMN_NAME_NOT_EXIST;  // 如果某个列不存在，返回错误
        }
    }

    // 检查索引是否已经存在
    if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
        return DB_INDEX_ALREADY_EXIST;  // 如果索引已存在，返回错误
    }

    // 创建索引元数据
    index_id_t index_id = next_index_id_++;
    IndexMetadata *index_metadata = IndexMetadata::Create(index_id, index_name, table_info->GetTableId(), {});

    // 创建索引信息
    index_info = IndexInfo::Create();
    index_info->Init(index_metadata, table_info, buffer_pool_manager_);

    // 将索引添加到目录中
    indexes_[index_id] = index_info;
    index_names_[table_name][index_name] = index_id;

    // 持久化索引元数据
    FlushCatalogMetaPage();  // 修改为正确的刷新方法

    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
    auto table_iter = index_names_.find(table_name);
    if (table_iter == index_names_.end()) {
        return DB_TABLE_NOT_EXIST;
    }

auto table_iter = index_names_.find(table_name);  // 先查找表名对应的索引映射
if (table_iter == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
}
auto index_iter = table_iter->second.find(index_name);  // 查找索引名
if (index_iter == table_iter->second.end()) {
    return DB_INDEX_NOT_FOUND;
}
index_info = indexes_.at(index_iter->second);  // 获取IndexInfo
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
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
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
    auto it = indexes_.find(index_name);
    if (it == indexes_.end()) {
        return DB_INDEX_NOT_FOUND;
    }

    IndexInfo *index_info = it->second;
    delete index_info;

    indexes_.erase(it);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
    char *buf = new char[PAGE_SIZE];
    catalog_meta_->SerializeTo(buf);
    page_id_t meta_page_id = CATALOG_META_PAGE_ID;
    disk_manager_->WritePage(meta_page_id, buf);
    delete[] buf;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
    // 读取表的元数据
    char *buf = new char[PAGE_SIZE];
    disk_manager_->ReadPage(page_id, buf);

    TableMetadata *table_metadata = nullptr;
    TableMetadata::DeserializeFrom(buf, table_metadata);
    delete[] buf;

    // 创建 TableInfo 对象
    TableInfo *table_info = new TableInfo(table_metadata);
    tables_[table_id] = table_info;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
    // 读取索引的元数据
    char *buf = new char[PAGE_SIZE];
    disk_manager_->ReadPage(page_id, buf);

    IndexMetadata *index_metadata = nullptr;
    IndexMetadata::DeserializeFrom(buf, index_metadata);
    delete[] buf;

    // 创建 IndexInfo 对象
    IndexInfo *index_info = new IndexInfo();
    index_info->Init(index_metadata, nullptr, buffer_pool_manager_);
    indexes_[index_id] = index_info;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
    auto it = tables_.find(table_id);
    if (it != tables_.end()) {
        table_info = it->second;
        return DB_SUCCESS;
    }
    return DB_TABLE_NOT_EXIST;
}
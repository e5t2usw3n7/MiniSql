#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
// 构造函数：初始化迭代器时检查是否需要加载数据
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) 
  : table_heap_(table_heap), rid_(rid), txn_(txn) {
  if (rid_.GetPageId() != INVALID_PAGE_ID) {
    row_ = new Row(rid_);
    if (!table_heap_->GetTuple(row_, txn_)) {
      delete row_;
      row_ = nullptr;
      rid_ = RowId(INVALID_PAGE_ID, -1);  // 设置无效RowId
    }
  } else {
    row_ = nullptr;
  }
}

// 复制构造函数
TableIterator::TableIterator(const TableIterator &other) {
  if (other.row_ != nullptr) {
    row_ = new Row(*other.row_);
  } else {
    row_ = nullptr;
  }
}

TableIterator::~TableIterator() {
  delete row_;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  return rid_ == itr.rid_;
  
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  ASSERT(row_ != nullptr, "Dereferencing invalid iterator");
  return *row_;  // 返回指向当前记录的引用
}

Row *TableIterator::operator->() {
  ASSERT(row_ != nullptr, "Dereferencing invalid iterator");
  return row_;  // 返回当前记录的指针
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this != &itr) {
    delete row_;  // 删除当前的row对象
    table_heap_ = itr.table_heap_;
    rid_ = itr.rid_;
    txn_ = itr.txn_;
    if (itr.row_ != nullptr) {
      row_ = new Row(*itr.row_);
    } else {
      row_ = nullptr;
    }
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (rid_.GetPageId() == INVALID_PAGE_ID) {
    return *this;  // 如果当前是无效的RowId，则不移动
  }

  TablePage *page = reinterpret_cast<TablePage *>(
      table_heap_->buffer_pool_manager_->FetchPage(rid_.GetPageId()));  // 获取当前页
  if (page == nullptr) {
    rid_ = RowId(INVALID_PAGE_ID, -1);  // 无效RowId
    delete row_;
    row_ = nullptr;
    return *this;
  }

  page->RLatch();  // 加锁
  RowId next_rid;  // 下一个记录的RowId
  if (!page->GetNextTupleRid(rid_, &next_rid)) {
    page->RUnlatch();
    table_heap_->buffer_pool_manager_->UnpinPage(rid_.GetPageId(), false);  // 解锁
    rid_ = RowId(INVALID_PAGE_ID, -1);  // 当前页没有下一条记录，标记为无效
  } else {
    rid_ = next_rid;  // 移动到下一条记录
    delete row_;
    row_ = nullptr;
  }

  // 加载下一条记录
  if (rid_.GetPageId() != INVALID_PAGE_ID) {
    row_ = new Row(rid_);
    if (!table_heap_->GetTuple(row_, txn_)) {
      delete row_;
      row_ = nullptr;
      rid_ = RowId(INVALID_PAGE_ID, -1);  // 如果没有找到记录，标记为无效
    }
  }

  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator clone(*this);  // 复制当前迭代器
  ++(*this);  // 使用前置++操作符
  return clone;  // 返回原始迭代器
}



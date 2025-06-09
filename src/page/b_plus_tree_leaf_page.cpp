#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE); // 设置为叶子页
  SetPageId(page_id);                    // 设置当前页ID
  SetParentPageId(parent_id);            // 设置父页ID
  SetKeySize(key_size);                  // 设置键大小
  SetSize(0);                            // 初始大小为0
  SetMaxSize(max_size);                  // 设置最大容量
  SetNextPageId(INVALID_PAGE_ID);        // 初始化下一个页ID为无效值
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  // 二分查找第一个 >= key 的位置
  int left = 0, right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    GenericKey *mid_key = KeyAt(mid);
    if (KM.CompareKeys(mid_key, key) < 0) {
      left = mid + 1;
    } else {
      right = mid - 1;
    }
  }
  return left; // 返回插入位置
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM); // 找到插入位置
  if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
    return GetSize(); // 键已存在，直接返回当前大小
  }
  // 移动后续键值对腾出空间
  PairCopy(PairPtrAt(index + 1), PairPtrAt(index), GetSize() - index);
  // 插入新键值对
  SetKeyAt(index, key);
  SetValueAt(index, value);
  IncreaseSize(1); // 更新大小
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int split_index = GetMinSize(); // 分裂点
  recipient->CopyNFrom(PairPtrAt(split_index), GetSize() - split_index);
  SetSize(split_index); // 更新当前页大小
  // 更新链表指针
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  int start = GetSize();
  PairCopy(PairPtrAt(start), src, size); // 复制数据
  IncreaseSize(size); // 更新大小
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index < GetSize() && KM.CompareKeys(KeyAt(index), key) == 0) {
    value = ValueAt(index); // 返回对应的RowId
    return true;
  }
  return false; // 键不存在
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int index = KeyIndex(key, KM);
  if (index >= GetSize() || KM.CompareKeys(KeyAt(index), key) != 0) {
    return GetSize(); // 键不存在，直接返回当前大小
  }
  // 移动后续键值对覆盖被删除项
  PairCopy(PairPtrAt(index), PairPtrAt(index + 1), GetSize() - index - 1);
  IncreaseSize(-1); // 更新大小
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(PairPtrAt(0), GetSize());
  recipient->SetNextPageId(GetNextPageId()); // 维护链表指针
  SetSize(0); // 清空当前页
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  // 1. 获取第一个键值对
  GenericKey *first_key = KeyAt(0);
  RowId first_value = ValueAt(0);

  // 2. 将第一个键值对插入到接收页的末尾
  recipient->CopyLastFrom(first_key, first_value);

  // 3. 删除当前页的第一个键值对（通过移动后续元素覆盖）
  PairCopy(PairPtrAt(0), PairPtrAt(1), GetSize() - 1);
  IncreaseSize(-1); // 更新大小
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  SetKeyAt(GetSize(), key);
  SetValueAt(GetSize(), value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  // 1. 获取最后一个键值对
  int last_index = GetSize() - 1;
  GenericKey *last_key = KeyAt(last_index);
  RowId last_value = ValueAt(last_index);

  // 2. 将最后一个键值对插入到接收页的头部
  recipient->CopyFirstFrom(last_key, last_value);

  // 3. 删除当前页的最后一个键值对
  IncreaseSize(-1); // 直接减少大小即可
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  PairCopy(PairPtrAt(1), PairPtrAt(0), GetSize());
  SetKeyAt(0, key);
  SetValueAt(0, value);
  IncreaseSize(1);
}
#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // Step 1: 获取索引根页（INDEX_ROOTS_PAGE_ID）
  auto root_info = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID));

  // Step 2: 从元数据中读取当前索引的根页面ID
  if (root_info->GetRootId(index_id, &root_page_id_)) {
    // 已存在根节点
  } else {
    // 没有找到对应的根页面
    root_page_id_ = INVALID_PAGE_ID;
  }

  // Step 3: 如果未指定大小，则根据页大小自动计算最大容量
  if (leaf_max_size_ == 0) {
    int key_size = processor_.GetKeySize();
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (key_size + sizeof(RowId));
  }

  if (internal_max_size_ == 0) {
    int key_size = processor_.GetKeySize();
    internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (key_size + sizeof(page_id_t));
  }

  // Step 4: 解除对根信息页的 pin
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  // 如果是无效，代表它就是根
  if (current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
  }
  if (root_page_id_ != INVALID_PAGE_ID) {
    auto root_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
    root_page->Delete(index_id_);
    buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
  }

  Page *page = buffer_pool_manager_->FetchPage(current_page_id);
  if (page == nullptr)  return;

  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (!node->IsLeafPage()) {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    for (int i = 0; i < internal->GetSize(); i ++) {
      Destroy(internal->ValueAt(i));
    }
  }

  buffer_pool_manager_->UnpinPage(current_page_id, true);
  buffer_pool_manager_->DeletePage(current_page_id);

  root_page_id_ = INVALID_PAGE_ID;

}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  Page *page = FindLeafPage(key, root_page_id_);
  if (IsEmpty() || !page) return false;
  auto *leaf = reinterpret_cast<LeafPage *>(page->GetData());

  RowId row_id;
  bool found = leaf->Lookup(key, row_id, processor_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  if (found) {
    result.push_back(row_id);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  // 分配新页面
  page_id_t new_page_id;
  Page *page = buffer_pool_manager_->NewPage(new_page_id);

  // 检查内存分配是否成功
  if (page == nullptr) throw std::overflow_error("Error: Out of memory, can't build a new tree");


  // 更新当前树的根页面 ID
  root_page_id_ = new_page_id;
  // 将该页解释为叶子节点
  auto *leaf_node = reinterpret_cast<LeafPage *>(page->GetData());

  // 初始化叶子节点
  leaf_node->Init(new_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  // 设置 next_page_id 为无效（因为这是唯一的叶子页）
  leaf_node->SetNextPageId(INVALID_PAGE_ID);

  // 插入第一个键值对
  int inserted_count = leaf_node->Insert(key, value, processor_);
  if (inserted_count < 0) {
    // 可选：处理插入失败的情况（比如 key 已存在）
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(page->GetPageId());
    throw std::runtime_error("Failed to insert first key-value pair into new B+ Tree");
  }

  // 解除 pin，并标记为 dirty（因为内容被修改了）
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  // 持久化根页面信息到 header page
  UpdateRootPageId(true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  Page *leaf_page = FindLeafPage(key, root_page_id_, false);
  if (leaf_page == nullptr) {
    return false;
  }
  RowId row_id;
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  // 如果插入后小于（最大个数 - 1），那么就不分裂
  if (leaf->GetSize() < leaf->GetMaxSize() - 1) {
    leaf->Insert(key, value, processor_);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }
  // 超过的情况，要分裂
  leaf->Insert(key, value, processor_);
  if (leaf->GetSize() == leaf->GetMaxSize()) {
    LeafPage *new_leaf = Split(leaf, transaction);
    GenericKey *new_key = new_leaf->KeyAt(0);
    InsertIntoParent(leaf, new_key, new_leaf, transaction);
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) throw std::overflow_error("Error: Out of memory, can't build a new page");
  auto *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());

  new_node->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  node->MoveHalfTo(new_node, buffer_pool_manager_);
  // 解除 pin，并标记为 dirty（因为内容被修改了）
  buffer_pool_manager_->UnpinPage(new_page_id, true);

  return new_node;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(new_page_id);
  if (new_page == nullptr) throw std::overflow_error("Error: Out of memory, can't build a new page");
  auto *new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());

  new_leaf->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  node->MoveHalfTo(new_leaf);
  new_leaf->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page->GetPageId());
  // 解除 pin，并标记为 dirty（因为内容被修改了）
  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);         // 新页
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);             // 原页

  return new_leaf;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) {
    // 创建新的根节点
    Page *new_page = buffer_pool_manager_->NewPage(root_page_id_);
    auto *new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);

    // 设置子节点
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    // 更新子节点的父指针
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);

    // unpin 旧节点和新节点，Update 默认根节点
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  }

  // 获取父节点
  page_id_t parent_id = old_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 在父节点中插入新的键和指针
  parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  new_node->SetParentPageId(parent_id);

  // 插入完成后 unpin 旧节点和新节点
  buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);

  // 检查是否需要分裂父节点
  if (parent->GetSize() >= internal_max_size_) {
    InternalPage *new_parent = Split(parent, transaction);
    GenericKey *new_key = new_parent->KeyAt(0);
    InsertIntoParent(parent, new_key, new_parent, transaction);
    buffer_pool_manager_->UnpinPage(new_parent->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(parent_id, true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) return; // If empty, return
  Page *leaf_page = FindLeafPage(key, root_page_id_, false);
  if (leaf_page == nullptr) return;  // Key not found
  auto *leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());

  int old_size = leaf->GetSize();
  int new_size = leaf->RemoveAndDeleteRecord(key, processor_);
  if (new_size == -1) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  if (new_size == leaf->GetSize()) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  // Case 1: 如果是根节点
  if (leaf->IsRootPage()) {
    if (leaf->GetSize() == 0 && AdjustRoot(leaf)) {
      buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
      buffer_pool_manager_->DeletePage(leaf->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return;
  }
  // Case 2: 非根节点
  bool should_update_parent = (new_size >= 0);  // Only update if leaf is non-empty
  if (new_size < leaf->GetMinSize()) {
    CoalesceOrRedistribute(leaf, transaction);
    return;
  } else if (should_update_parent) {
    // 如果没有发生合并或重分布，则更新浮木节点的分隔键
    page_id_t ParentId = leaf->GetParentPageId();
    Page *parent_page = buffer_pool_manager_->FetchPage(ParentId);
    auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
    int node_index = parent_node->ValueIndex(leaf->GetPageId());

    // 只有当不是最左子节点时才更新（内部节点中的第一个键是虚拟的）
    if (node_index > 0) {
      parent_node->SetKeyAt(node_index, leaf->KeyAt(0));
    }

    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *txn) {
  // Step 1: 如果是根节点，尝试调整根
  if (node->IsRootPage() && (!AdjustRoot(node))) {
    return false;
  } else if (node->IsRootPage() && AdjustRoot(node)) {
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
    return true;
  }

  // Step 2: 获取父节点
  Page *parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto *parent = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // Step 3: 获取当前节点在父节点中的索引
  int node_index = parent->ValueIndex(node->GetPageId());

  // Step 4: 如果是叶子节点，更新父节点中对应 key
  if (node->IsLeafPage()) {
    parent->SetKeyAt(node_index, node->KeyAt(0));
  }

  // Step 5: 根据当前节点的位置决定如何处理
  bool coalesce = false;
  int parentsize = parent->GetSize();
  // 定义处理策略
  // 最左边节点：只能找右边的兄弟
  auto handle_leftmost = [&]() {
    N* right = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent->ValueAt(1)));
    if (right->GetSize() + node->GetSize() <= node->GetMaxSize()) {
      coalesce = Coalesce(right, node, parent, node_index, txn);
    } else {
      Redistribute(right, node, 0); // 向右借
    }
    buffer_pool_manager_->UnpinPage(right->GetPageId(), true);
  };

  // 最右边节点：只能找左边的兄弟
  auto handle_rightmost = [&]() {
    N* left = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent->ValueAt(node_index - 1)));
    if (left->GetSize() + node->GetSize() <= node->GetMaxSize()) {
      coalesce = Coalesce(node, left, parent, node_index - 1, txn);
    } else {
      Redistribute(left, node, 1); // 向左借
    }
    buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
  };

  // 中间节点：尝试左右两个兄弟
  auto handle_middle = [&]() {
    N* left = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent->ValueAt(node_index - 1)));
    N* right = reinterpret_cast<N*>(buffer_pool_manager_->FetchPage(parent->ValueAt(node_index + 1)));

    bool use_left = (left->GetSize() + node->GetSize() <= node->GetMaxSize());
    bool use_right = (right->GetSize() + node->GetSize() <= node->GetMaxSize());

    if (use_left) {
      buffer_pool_manager_->UnpinPage(right->GetPageId(), false);
      coalesce = Coalesce(node, left, parent, node_index - 1, txn);
    } else if (use_right) {
      buffer_pool_manager_->UnpinPage(left->GetPageId(), false);
      coalesce = Coalesce(node, right, parent, node_index, txn);
    } else {
      buffer_pool_manager_->UnpinPage(right->GetPageId(), false);
      buffer_pool_manager_->UnpinPage(left->GetPageId(), false);
      Redistribute(left, node, 1);
    }

    if (!use_left) buffer_pool_manager_->UnpinPage(left->GetPageId(), true);
  };

  // 策略选择
  if (node_index == 0) {
    handle_leftmost();
  } else if (node_index == parentsize - 1) {
    handle_rightmost();
  } else {
    handle_middle();
  }

  // Step 6: 释放资源
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);

  return coalesce;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  // Step 1: 移动所有记录到邻居节点
  node->MoveAllTo(neighbor_node);

  // 更新叶子链表 next 指针
  // neighbor_node->SetNextPageId(node->GetNextPageId()); MoveAllTo干了这个操作了

  // Step 2: 删除父节点中对应的 key 和子节点指针
  parent->Remove(index);

  // Step 3: 释放当前节点资源
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  int parentsize = parent->GetSize(); // 先获取父节点信息
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  // Step 4: 判断父节点是否下溢
  if (parentsize < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
  return false;
}

// 内部节点合并 (Coalesce Internal)
bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  // 删除父节点中对应的 key 和子节点指针
  parent->Remove(index);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  int parentsize = parent->GetSize();
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  if (parentsize < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  } else {
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  page_id_t parent_id = neighbor_node->GetParentPageId();
  Page *parent = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent->GetData());

  LeafPage *temp;
  if (index == 0) {
    // 将右兄弟的第一个元素移动到末尾
    neighbor_node->MoveFirstToEndOf(node);
    temp = neighbor_node;
  } else {
    // 将左兄弟的最后一个元素移动到头部
    neighbor_node->MoveLastToFrontOf(node);
    temp = node;
  }

  // 找到当前 node 在父节点中的位置
  int node_index = parent_node->ValueIndex(temp->GetPageId());

  // 更新父节点的 key
  node->SetKeyAt(node_index, temp->KeyAt(0));

  // unpin 解锁
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  // 获取父节点
  page_id_t parent_id = neighbor_node->GetParentPageId();
  Page *parent_page = buffer_pool_manager_->FetchPage(parent_id);
  auto *parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int node_index = 0;
  InternalPage *temp;
  // index=0表示node是neighbor的右兄弟，否则是左兄弟
  if (index == 0) {
    temp = neighbor_node;
    node_index = parent_node->ValueIndex(temp->GetPageId());
    // 将右兄弟的第一个元素移动到当前节点的末尾
    neighbor_node->MoveFirstToEndOf(node, parent_node->KeyAt(node_index), buffer_pool_manager_);
  } else {
    temp = node;
    node_index = parent_node->ValueIndex(temp->GetPageId());
    // 将左兄弟的最后一个元素移动到当前节点的头部
    neighbor_node->MoveLastToFrontOf(node, parent_node->KeyAt(node_index), buffer_pool_manager_);
  }
  // 更新父节点的分隔键
  parent_node->SetKeyAt(node_index, temp->KeyAt(0));

  buffer_pool_manager_->UnpinPage(parent_id, true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  // 情况1：删除后根节点仍有子节点
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {
    auto *internal = reinterpret_cast<InternalPage *>(old_root_node);
    root_page_id_ = internal->ValueAt(0);

    // 更新新根节点的父指针
    Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
    auto *new_root = reinterpret_cast<BPlusTreePage *>(page->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);

    // 释放旧根节点
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());

    UpdateRootPageId(0);
    return true;
  }
  // 情况2：树中最后一个元素被删除
  else if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  // 情况3：不应到达的地方
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  Page *page = FindLeafPage(nullptr, INVALID_PAGE_ID, true);
  if (page == nullptr) {return IndexIterator();}
  int page_id = page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  Page *page = FindLeafPage(key, INVALID_PAGE_ID, true);
  if (page == nullptr) {return IndexIterator();}
  int page_id = page->GetPageId();
  auto *node = reinterpret_cast<LeafPage *>(page);
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, node->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  auto *node = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, INVALID_PAGE_ID, true));
  BPlusTreeLeafPage *next_node;
  if (node == nullptr) {return IndexIterator();}
  while (node->GetNextPageId() != INVALID_PAGE_ID) {
    next_node = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(node->GetNextPageId()));
    buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
    node = next_node;
  }
  page_id_t page_id = node->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, node->GetSize());
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if (page_id == INVALID_PAGE_ID) {
    page_id = root_page_id_;
  }
  if (page_id == INVALID_PAGE_ID) {
    return nullptr;
  }

  Page *page = buffer_pool_manager_->FetchPage(page_id);
  auto *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  // 递归查找直到叶子节点
  while (!node->IsLeafPage()) {
    auto *internal = reinterpret_cast<InternalPage *>(node);
    page_id_t child_page_id;

    if (leftMost) {
      child_page_id = internal->ValueAt(0);
    } else {
      child_page_id = internal->Lookup(key, processor_);
    }

    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = child_page_id;
    page = buffer_pool_manager_->FetchPage(page_id);
    node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  }

  return page;
}

/*
 * Update/Insert root page id in header page(where page_id = INDEX_ROOTS_PAGE_ID,
 * header_page isdefined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *page = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  auto *root_page = reinterpret_cast<IndexRootsPage *>(page->GetData());

  if (insert_record) {
    root_page->Insert(index_id_, root_page_id_);
  } else {
    root_page->Update(index_id_, root_page_id_);
  }

  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
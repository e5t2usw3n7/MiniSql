#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i ++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::lock_guard<std::recursive_mutex> lock(latch_);

  // 1. 检查是否已在缓冲池中
  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    frame_id_t frame_id = it->second;
    pages_[frame_id].pin_count_++;
    replacer_->Pin(frame_id);
    return &pages_[frame_id];
  }

  // 2. 获取可用frame
  frame_id_t frame_id = TryToFindFreePage();
  if (frame_id == -1) {
    return nullptr;  // 没有可用页
  }

  // 3. 准备新页
  page_table_[page_id] = frame_id;
  Page *page = &pages_[frame_id];
  disk_manager_->ReadPage(page_id, page->data_);
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;

  // 5. pin
  replacer_->Pin(frame_id);

  return page;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  std::lock_guard<std::recursive_mutex> lock(latch_);

  // 1. 获取可用frame
  frame_id_t frame_id = TryToFindFreePage();
  if (frame_id == -1) {
    return nullptr;
  }

  // 2. 分配新页号
  page_id = AllocatePage();

  // 3. 初始化新页
  Page *page = &pages_[frame_id];
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();

  // 4. 更新元数据
  page_table_[page_id] = frame_id;
  replacer_->Pin(frame_id);

  return page;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  std::lock_guard<std::recursive_mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return true;  // 页不存在，视为删除成功
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  if (page->pin_count_ > 0) {
    return false;  // 页正在使用
  }

  // 表明删除，解除缓冲池对该页的引用
  page_table_.erase(page_id);

  // 重置页状态
  page->page_id_ = INVALID_PAGE_ID;
  page->pin_count_ = 0;
  page->is_dirty_ = false;
  page->ResetMemory();

  // 更新元数据
  free_list_.push_back(frame_id);
  // 从磁盘删除
  DeallocatePage(page_id);

  return true;
}

/**
 * TODO: Student Implement
 */

// 取消固定页
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  // 使用递归互斥锁保护共享数据结构，防止多线程并发访问导致的数据不一致
  std::lock_guard<std::recursive_mutex> lock(latch_);

  // 在页表中查找指定的page_id
  auto it = page_table_.find(page_id);

  // 如果没有找到该page_id对应的条目，则返回false，表示页不存在
  if (it == page_table_.end()) {
    return false;  // 页不存在
  }

  // 获取对应的frame_id
  frame_id_t frame_id = it->second;
  // 获取对应的页面指针
  Page *page = &pages_[frame_id];

  // 如果页面的pin计数小于等于0，表示页面未被固定，返回false
  if (page->pin_count_ <= 0) {
    return false;  // 页未被固定
  }

  // 减少页面的pin计数
  if ((--(page->pin_count_)) == 0) {
    // 如果pin计数变为0，通知replacer该页面不再被固定
    replacer_->Unpin(frame_id);
  }

  page->is_dirty_ = is_dirty;

  // 成功解除固定页面，返回true
  return true;
}

/**
 * TODO: Student Implement
 */

// 刷新页到磁盘
bool BufferPoolManager::FlushPage(page_id_t page_id) {

  std::lock_guard<std::recursive_mutex> lock(latch_);

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;  // 页不存在
  }

  frame_id_t frame_id = it->second;
  Page *page = &pages_[frame_id];

  disk_manager_->WritePage(page_id, page->data_);
  page->is_dirty_ = false;

  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
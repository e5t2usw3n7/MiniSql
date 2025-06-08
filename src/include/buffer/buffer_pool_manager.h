#ifndef MINISQL_BUFFER_POOL_MANAGER_H
#define MINISQL_BUFFER_POOL_MANAGER_H

#include <list>
#include <mutex>
#include <unordered_map>

#include "buffer/lru_replacer.h"
#include "page/disk_file_meta_page.h"
#include "page/page.h"
#include "storage/disk_manager.h"

using namespace std;

class BufferPoolManager {
 public:
  explicit BufferPoolManager(size_t pool_size, DiskManager *disk_manager);

  ~BufferPoolManager();

  Page *FetchPage(page_id_t page_id);

  bool UnpinPage(page_id_t page_id, bool is_dirty);

  bool FlushPage(page_id_t page_id);

  Page *NewPage(page_id_t &page_id);

  bool DeletePage(page_id_t page_id);

  bool IsPageFree(page_id_t page_id);

  bool CheckAllUnpinned();

 private:
  /**
   * Allocate new page (operations like create index/table) For now just keep an increasing counter
   */
  page_id_t AllocatePage();

  /**
   * Deallocate page (operations like drop index/table) Need bitmap in header page for tracking pages
   */
  void DeallocatePage(page_id_t page_id);

  frame_id_t TryToFindFreePage() {
    frame_id_t frame_id = -1;

    // 首先尝试从free_list_获取
    if (!free_list_.empty()) {
      frame_id = free_list_.front();
      free_list_.pop_front();
      return frame_id;
    }

    // 如果没有空闲页，尝试从replacer_获取
    if (replacer_->Victim(&frame_id)) {
      // 如果找到victim，检查是否是脏页需要写回
      if (pages_[frame_id].IsDirty()) {
        disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
      }
      // 从page_table_中移除旧映射
      page_table_.erase(pages_[frame_id].GetPageId());
      return frame_id;
    }

    // 没有可用页
    return -1;
  }

 private:
  size_t pool_size_;                                 // number of pages in buffer pool
  Page *pages_;                                      // array of pages
  DiskManager *disk_manager_;                        // pointer to the disk manager.
  unordered_map<page_id_t, frame_id_t> page_table_;  // to keep track of pages
  Replacer *replacer_;                               // to find an unpinned page for replacement
  list<frame_id_t> free_list_;                       // to find a free page for replacement
  recursive_mutex latch_;                            // to protect shared data structure
};

#endif  // MINISQL_BUFFER_POOL_MANAGER_H

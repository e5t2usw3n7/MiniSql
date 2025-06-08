#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage() {
  // 获取元数据页的指针
  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);

  // 尝试在现有分区中分配页面
  for (uint32_t extent_id = 0; extent_id < meta_page->num_extents_; extent_id++) {
    // 检查当前分区是否还有空闲页面
    if (meta_page->extent_used_page_[extent_id] < BITMAP_SIZE) {
      // 计算位图页面的ID
      page_id_t bitmap_page_id = 1 + extent_id * (1 + BITMAP_SIZE);

      // 读取位图页面的数据
      char bitmap_data[PAGE_SIZE];
      ReadPhysicalPage(bitmap_page_id, bitmap_data);
      auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);

      // 在位图中分配一个页面
      uint32_t page_offset;
      if (bitmap->AllocatePage(page_offset)) {
        // 更新元数据中的已使用页面数量
        meta_page->extent_used_page_[extent_id]++;
        // 更新元数据中的总分配页面数量
        meta_page->num_allocated_pages_++;
        // 将更新后的位图写回磁盘
        WritePhysicalPage(bitmap_page_id, bitmap_data);
        // 返回新分配页面的ID
        return extent_id * BITMAP_SIZE + page_offset;
      }
    }
  }

  // 如果所有现有分区都满了，则创建一个新的分区
  uint32_t new_extent_id = meta_page->num_extents_;
  // 计算新分区的位图页面ID
  page_id_t new_bitmap_page_id = 1 + new_extent_id * (1 + BITMAP_SIZE);

  // 初始化新的位图页面
  BitmapPage<PAGE_SIZE> new_bitmap;
  uint32_t page_offset;
  // 在新位图中分配一个页面
  new_bitmap.AllocatePage(page_offset);

  // 更新元数据中的新分区信息
  meta_page->extent_used_page_[new_extent_id] = 1;
  meta_page->num_extents_++;
  meta_page->num_allocated_pages_++;

  // 将新位图页面写入磁盘
  WritePhysicalPage(new_bitmap_page_id, reinterpret_cast<char *>(&new_bitmap));

  // 返回新分配页面的ID
  return new_extent_id * BITMAP_SIZE + page_offset;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  // 计算要释放页面所在的分区ID
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  // 计算要释放页面在分区中的偏移量
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;

  // 获取元数据页的指针
  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  // 如果分区ID超出了当前存在的分区数量，则直接返回
  if (extent_id >= meta_page->num_extents_) return;

  // 计算位图页面的ID
  page_id_t bitmap_page_id = 1 + extent_id * (1 + BITMAP_SIZE);
  // 定义一个字符数组来存储位图页面的数据
  char bitmap_data[PAGE_SIZE];
  // 从磁盘读取位图页面的数据
  ReadPhysicalPage(bitmap_page_id, bitmap_data);

  // 将读取到的位图数据转换为位图对象
  auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);
  // 在位图中释放指定偏移量的页面
  if (bitmap->DeAllocatePage(page_offset)) {
    // 更新元数据中的已使用页面数量
    meta_page->extent_used_page_[extent_id]--;
    // 更新元数据中的总分配页面数量
    meta_page->num_allocated_pages_--;
    // 将更新后的位图写回磁盘
    WritePhysicalPage(bitmap_page_id, bitmap_data);
  }
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;

  auto *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  if (extent_id >= meta_page->num_extents_) return true;

  page_id_t bitmap_page_id = 1 + extent_id * (1 + BITMAP_SIZE);
  char bitmap_data[PAGE_SIZE];
  ReadPhysicalPage(bitmap_page_id, bitmap_data);

  auto *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(bitmap_data);
  return bitmap->IsPageFree(page_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  // 每个extent能管理的数据页数量 = BITMAP_SIZE
  uint32_t extent_num = logical_page_id / BITMAP_SIZE;
  uint32_t page_offset = logical_page_id % BITMAP_SIZE;

  // 物理页号计算:
  // 1. 跳过meta page (0)
  // 2. 每个extent占 (1 bitmap page + BITMAP_SIZE data pages)
  return 1 + extent_num * (1 + BITMAP_SIZE) + 1 + page_offset;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
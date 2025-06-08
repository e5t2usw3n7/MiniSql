#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // 检查是否还有空闲页
  if (page_allocated_ >= GetMaxSupportedSize()) {
    return false;
  }

  // 从next_free_page_开始查找空闲页
  for (uint32_t i = next_free_page_; i < GetMaxSupportedSize(); i++) {
    uint32_t byte_index = i / 8;
    uint8_t bit_index = i % 8;

    if (IsPageFreeLow(byte_index, bit_index)) {
      // 标记为已分配
      bytes[byte_index] |= (1 << bit_index);
      page_allocated_++;
      page_offset = i;

      // 更新next_free_page_为下一个可能空闲的位置
      next_free_page_ = i + 1;
      if (next_free_page_ >= GetMaxSupportedSize()) {
        next_free_page_ = 0; // 循环回到开头
      }

      return true;
    }
  }

  // 如果从next_free_page_开始没找到，从开头再找
  for (uint32_t i = 0; i < next_free_page_; i++) {
    uint32_t byte_index = i / 8;
    uint8_t bit_index = i % 8;

    if (IsPageFreeLow(byte_index, bit_index)) {
      bytes[byte_index] |= (1 << bit_index);
      page_allocated_++;
      page_offset = i;
      next_free_page_ = i + 1;
      return true;
    }
  }

  // 不应该执行到这里，因为page_allocated_ < GetMaxSupportedSize()
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  }

  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  if (IsPageFreeLow(byte_index, bit_index) || IsPageFree(page_offset)) {
    return false;
  }

  // 标记为未分配
  bytes[byte_index] &= ~(1 << bit_index);
  page_allocated_--;

  // 更新next_free_page_为可能更小的空闲位置
  if (page_offset < next_free_page_) {
    next_free_page_ = page_offset;
  }

  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if (page_offset >= GetMaxSupportedSize()) {
    return false;
  }

  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
// 检查bitmap中特定位的状态
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  return ((bytes[byte_index] & (1 << bit_index)) == 0);
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
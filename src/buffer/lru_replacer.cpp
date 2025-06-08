#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  // 使用锁保护共享数据结构，防止多线程并发访问导致的数据不一致
  std::lock_guard<std::mutex> lock(latch_);

  // 检查LRU列表是否为空，如果为空则无法找到受害者帧，返回false
  if (lru_list_.empty()) {
    return false;
  }

  // 获取并移除最近最少使用的frame（链表尾部）
  *frame_id = lru_list_.back();  // 获取链表最后一个元素（LRU算法里即最近最少使用的帧）
  lru_list_.pop_back();          // 移除链表的最后一个元素
  lru_map_.erase(*frame_id);     // 删除对应的条目

  // 成功找到并移除受害者帧，返回true
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  // 使用锁保护共享数据结构，防止多线程并发访问导致的数据不一致
  std::lock_guard<std::mutex> lock(latch_);

  // 如果没找到直接返回
  if (lru_map_.count(frame_id) == 0)  return;
  // 在哈希map映射中查找指定的frame_id
  auto it = lru_map_.find(frame_id);
  // 如果找到了该frame_id对应的条目
  if (it != lru_map_.end()) {
    // 从LRU列表中移除该frame（因为pin操作表示该frame正在被使用）
    lru_list_.erase(it->second);
    // 从哈希映射中删除该frame对应的条目
    lru_map_.erase(it);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // 检查frame是否已经在LRU中
  if (lru_map_.find(frame_id) != lru_map_.end()) {
    return;  // 已在LRU中，无需重复添加
  }

  // 添加到LRU列表头部（表示最近使用）
  lru_list_.push_front(frame_id);
  lru_map_[frame_id] = lru_list_.begin();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  std::lock_guard<std::mutex> lock(latch_);
  return lru_list_.size();
}
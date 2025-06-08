#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t size = 0;
  for (auto column : columns_) {
    size += column->SerializeTo(buf + size);
  }
  return size;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = 0;
  for (auto column : columns_) {
    size += column->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;
  std::vector<Column *> columns;
  while (offset < PAGE_SIZE) {
    Column *column;
    offset += Column::DeserializeFrom(buf + offset, column);
    columns.push_back(column);
  }
  schema = new Schema(columns);
  return offset;
}
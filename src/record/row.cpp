#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t size = 0;
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    size += fields_[i]->SerializeTo(buf + size);
  }
  return size;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t offset = 0;
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &fields_[i], false);
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t size = 0;
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    size += fields_[i]->GetSerializedSize();
  }
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}

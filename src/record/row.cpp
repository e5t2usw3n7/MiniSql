#include "record/row.h"

/**
 * Serialize Row to buffer
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  uint32_t size = 0;
  // Serialize each field in the row
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    // Serialize the current field into the buffer
    size += fields_[i]->SerializeTo(buf + size);
  }
  return size;
}

/**
 * Deserialize Row from buffer
 */
uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  uint32_t offset = 0;
  
  // Initialize fields_ to the size of schema's columns
  fields_.clear();
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    // Allocate a new Field for each column
    Field* field = nullptr;
    offset += Field::DeserializeFrom(buf + offset, schema->GetColumn(i)->GetType(), &field, false);
    fields_.push_back(field);  // Add the deserialized field to the fields_ vector
  }

  return offset;
}

/**
 * Get the size of the serialized Row
 */
uint32_t Row::GetSerializedSize(Schema *schema) const {
  uint32_t size = 0;
  
  // Get the size of each field in the row
  for (uint32_t i = 0; i < schema->GetColumnCount(); ++i) {
    size += fields_[i]->GetSerializedSize();
  }

  return size;
}

/**
 * Generate a key Row based on the schema and key schema
 */
void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;

  uint32_t idx;
  // Generate the key row by using the columns in key_schema
  for (auto column : columns) {
    // Get the index of the column in the schema
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));  // Get the field from current row
  }

  // Set the key_row to a new Row with the generated fields
  key_row = Row(fields);
}

#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  memcpy(buf + offset, &COLUMN_MAGIC_NUM, sizeof(COLUMN_MAGIC_NUM));
  offset += sizeof(COLUMN_MAGIC_NUM);
  memcpy(buf + offset, name_.c_str(), name_.length() + 1);
  offset += name_.length() + 1;
  MACH_WRITE_TO(uint32_t, buf + offset, len_);
  offset += sizeof(len_);
  MACH_WRITE_TO(uint32_t, buf + offset, table_ind_);
  offset += sizeof(table_ind_);
  MACH_WRITE_TO(bool, buf + offset, nullable_);
  offset += sizeof(nullable_);
  MACH_WRITE_TO(bool, buf + offset, unique_);
  return offset + sizeof(unique_);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  return sizeof(COLUMN_MAGIC_NUM) + name_.length() + 1 + sizeof(len_) + sizeof(table_ind_) + sizeof(nullable_) + sizeof(unique_);
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset = 0;
  uint32_t magic;
  memcpy(&magic, buf + offset, sizeof(magic));
  offset += sizeof(magic);
  if (magic != COLUMN_MAGIC_NUM) {
    return 0;
  }
  column = new Column("", TypeId::kTypeInvalid, 0, false, false);
  column->name_ = std::string(buf + offset);
  offset += column->name_.length() + 1;
  column->len_ = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(column->len_);
  column->table_ind_ = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(column->table_ind_);
  column->nullable_ = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(column->nullable_);
  column->unique_ = MACH_READ_FROM(bool, buf + offset);
  return offset + sizeof(column->unique_);
}

#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t SerializedSize = 0;
  memcpy(buf, &SCHEMA_MAGIC_NUM, sizeof(uint32_t));
  SerializedSize += sizeof(uint32_t);

  uint32_t column_count;
  column_count = GetColumnCount();
  memcpy(buf + SerializedSize, &column_count, sizeof(uint32_t));
  SerializedSize += sizeof(uint32_t);

  for(auto it : columns_) {
    column_count = it->SerializeTo(buf + SerializedSize);
    SerializedSize += column_count;
  }
  // for(auto i = columns_.begin(); i != columns_.end(); i++){
  //   column_count = (*i)->SerializeTo(buf + SerializedSize);
  //   SerializedSize += column_count;
  // }

  memcpy(buf + SerializedSize, &is_manage_, sizeof(bool));
  SerializedSize += sizeof(bool);
  return SerializedSize;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t num = 0;

  // for(auto it : columns_) {
  //   num += it->GetSerializedSize();
  // }
  for(auto i = columns_.begin(); i != columns_.end(); i++){
    num += (*i)->GetSerializedSize();
  }
  uint32_t res = 2*sizeof(uint32_t) + sizeof(bool) + num;
  return res;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t SerializedSize = 0;
  std::vector<Column *> columns;

  uint32_t SCHEMA_MAGIC_NUM_Read = 0;
  memcpy(&SCHEMA_MAGIC_NUM_Read, buf, sizeof(uint32_t));
  SerializedSize += sizeof(uint32_t);
  if(SCHEMA_MAGIC_NUM_Read != SCHEMA_MAGIC_NUM){
    LOG(ERROR)<<"Deserialized From function in schema is WRONG!" << std::endl;
  }

  uint32_t column_count;
  memcpy(&column_count, buf + SerializedSize, sizeof(uint32_t));
  SerializedSize += sizeof(uint32_t);

  for(uint32_t i = 0; i < column_count; i++){
    Column* temp = nullptr;
    SerializedSize += Column::DeserializeFrom(buf + SerializedSize, temp);
    columns.push_back(temp);
    temp = nullptr;
  }

  bool is_manage;
  memcpy(&is_manage, buf + SerializedSize, sizeof(bool));
  SerializedSize += sizeof(bool);
  schema = new Schema(columns, is_manage);

  return SerializedSize;
}
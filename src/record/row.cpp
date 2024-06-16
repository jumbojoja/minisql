#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t SerializedSize = 0, count = 0;
  uint32_t field_count = GetFieldCount();
  memcpy(buf, &field_count, sizeof(uint32_t));  /*第一个被序列化的，本tuple有多少个field，4个字节*/
  SerializedSize += sizeof(uint32_t);
  if(field_count == 0){ //no fields
    return SerializedSize;
  }
  uint32_t byte_count = ceil(field_count*1.0/8);
  char* null_bitmaps = new char[byte_count];
  memset(null_bitmaps, 0, byte_count);
  for(auto i = fields_.begin(); i != fields_.end(); i++){
    /*这里用位运算去点bitmap了*/
    if(!((*i)->IsNull())){
      null_bitmaps[count/8] |= (1<<(7-(count%8)));
    }
    count++;
  }
  memcpy(buf + SerializedSize, null_bitmaps, byte_count*sizeof(char));  /*第二个被序列化的，bitmap，字节数等于field数/8取上整 */
  SerializedSize += byte_count*sizeof(char);
  for(auto i = fields_.begin(); i != fields_.end(); i++){
    uint32_t tmp = (*i)->SerializeTo(buf + SerializedSize);
    SerializedSize += tmp;
  }
  /* *  In memory:
   * -------------------------------------------
   * | Field Nums | Null bitmap | Field-1 | ... | Field-N |
   * -------------------------------------------
   */
  delete []null_bitmaps;
  return SerializedSize;
  //return 0;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  fields_.resize(0);
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here

  uint32_t SerializedSize = 0;
  uint32_t num = 0;
  memcpy(&num, buf, sizeof(uint32_t));
  SerializedSize += sizeof(uint32_t);
  if(!num){
    return SerializedSize;
  }
  uint32_t byte_count = ceil(num * 1.0/8);
  char* null_bitmaps = new char[byte_count];
  memcpy(null_bitmaps, buf + SerializedSize, byte_count*sizeof(char));
  SerializedSize += byte_count*sizeof(char);
  for(uint32_t i = 0; i < num; i++){
    Field *temp;
    TypeId type = schema->GetColumn(i)->GetType();
    if((null_bitmaps[i/8]&(1<<(7-(i%8)))) != 0){
      if(type == TypeId::kTypeInt){
        int32_t row_int = 0;
        memcpy(&row_int, buf + SerializedSize, sizeof(int));
        SerializedSize += sizeof(int);
        temp = new Field(type, row_int);
      }else if(type == TypeId::kTypeFloat){
        float row_float = 0;
        memcpy(&row_float, buf + SerializedSize, sizeof(float));
        SerializedSize += sizeof(float);
        temp = new Field(type, row_float);
      }else{
        uint32_t length = 0;
        memcpy(&length, buf + SerializedSize, sizeof(uint32_t));
        SerializedSize += sizeof(uint32_t);
        char* value = new char[length];
        memcpy(value, buf + SerializedSize, length);
        SerializedSize += length;
        temp = new Field(type, value, length, true);
        delete []value;
      }
    }else{
      temp = new Field(type);
    }
    fields_.push_back(temp);
  }
  delete []null_bitmaps;
  return SerializedSize;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t num = 0;
  for(auto i = fields_.begin(); i != fields_.end(); i++){
    num += (*i)->GetSerializedSize();
  }
  /*回想一个row被序列化之后的结构即可理解。field数量字节数+bitmap字节数+所有field各自序列化之后的总字节数*/
  return sizeof(uint32_t) + ceil(GetFieldCount()*1.0/8) + num;
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
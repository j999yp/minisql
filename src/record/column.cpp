#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique)
{
    ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
    switch (type)
    {
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
      unique_(unique)
{
    ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const
{
    // replace with your code here
    // magic_num(4), name_len(4), name(?), type(1), data_len(4), pos(4), nullable(1), unique(1)
    union
    {
        int32_t int_;
        uint32_t uint_;
        float_t float_;
        uint8_t data_[4];
    } tmp;

    char *origin = buf;
    // magic number, 4
    tmp.uint_ = COLUMN_MAGIC_NUM;
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);
    // name length, 4
    tmp.uint_ = GetName().length();
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);
    // name, ?
    memcpy(buf, GetName().data(), GetName().length());
    buf += GetName().length();
    // type, 1
    *(buf++) = (uint8_t)GetType();
    // data length, 4
    tmp.uint_ = GetLength();
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);
    // pos, 4
    tmp.uint_ = GetTableInd();
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);
    // nullable, 1
    *(buf++) = (uint8_t)IsNullable();
    // unique, 1
    *(buf++) = (uint8_t)IsUnique();

    return buf - origin;
}

uint32_t Column::GetSerializedSize() const
{
    // replace with your code here
    return 19 + GetName().length();
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column)
{
    union
    {
        int32_t int_;
        uint32_t uint_;
        float_t float_;
        uint8_t data_[4];
    } tmp;

    uint32_t magic_num, name_len, data_len, pos;
    uint8_t type, nullable, unique;
    char *origin = buf;

    memcpy(tmp.data_, buf, 4);
    magic_num = tmp.uint_;
    buf += sizeof(uint32_t);

    memcpy(tmp.data_, buf, 4);
    name_len = tmp.uint_;
    buf += sizeof(uint32_t);

    char name[name_len + 1] = {0};
    memcpy(name, buf, name_len);
    buf += name_len;

    memcpy(&type, buf, 1);
    buf += sizeof(uint8_t);

    memcpy(tmp.data_, buf, 4);
    data_len = tmp.uint_;
    buf += sizeof(uint32_t);

    memcpy(tmp.data_, buf, 4);
    pos = tmp.uint_;
    buf += sizeof(uint32_t);

    memcpy(&nullable, buf, 1);
    buf += sizeof(uint8_t);

    memcpy(&unique, buf, 1);
    buf += sizeof(uint8_t);

    ASSERT(magic_num == COLUMN_MAGIC_NUM, "Invalid magic num");
    if (type == TypeId::kTypeChar)
        column = new Column(std::string(name), (TypeId)type, data_len, pos, nullable, unique);
    else
        column = new Column(std::string(name), (TypeId)type, pos, nullable, unique);
    return buf - origin;
}

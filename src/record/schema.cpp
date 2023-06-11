#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const
{
    // replace with your code here
    union
    {
        int32_t int_;
        uint32_t uint_;
        float_t float_;
        uint8_t data_[4];
    } tmp;

    char *origin = buf;
    // magic number, 4
    tmp.uint_ = SCHEMA_MAGIC_NUM;
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);
    // is_manage, 1
    *(buf++) = (uint8_t)is_manage_;
    // column_num, 4
    tmp.uint_ = columns_.size();
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);
    // columns
    for (auto col : columns_)
    {
        buf += col->SerializeTo(buf);
    }
    return buf - origin;
}

uint32_t Schema::GetSerializedSize() const
{
    // replace with your code here
    int size = 9;
    for (auto it : columns_)
        size += it->GetSerializedSize();
    return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema)
{
    // replace with your code here
    union
    {
        int32_t int_;
        uint32_t uint_;
        float_t float_;
        uint8_t data_[4];
    } tmp;

    uint32_t magic_num, col_num;
    uint8_t is_manage;
    char *origin = buf;

    memcpy(tmp.data_, buf, 4);
    magic_num = tmp.uint_;
    buf += sizeof(uint32_t);

    memcpy(&is_manage, buf, 1);
    buf += sizeof(uint8_t);

    memcpy(tmp.data_, buf, 4);
    col_num = tmp.uint_;
    buf += sizeof(uint32_t);

    std::vector<Column *> columns(col_num, nullptr);
    for (auto it : columns)
    {
        buf += Column::DeserializeFrom(buf, it);
    }

    ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Invalid magic num");
    schema = new Schema(columns, is_manage);
    return buf - origin;
}
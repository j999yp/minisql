#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const
{
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    // replace with your code here
    union
    {
        int32_t int_;
        uint32_t uint_;
        float_t float_;
        uint8_t data_[4];
    } tmp;

    char *origin = buf;

    // magic_num, 4
    tmp.uint_ = ROW_MAGIC_NUM;
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);
    // page_id, 4
    tmp.uint_ = rid_.GetPageId();
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);
    // row_id, 4
    tmp.uint_ = rid_.GetSlotNum();
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(uint32_t);

    // null bitmap
    uint8_t map_num = fields_.size() / 8 + 1;
    tmp.uint_ = map_num;
    memcpy(buf, tmp.data_, 4);
    buf += sizeof(int32_t);

    char *bit_map_ptr = buf, bit_map = 0;
    buf += map_num;

    // fields
    for (int i = 0; i < fields_.size(); i++)
    {
        bit_map += fields_[i]->IsNull() ? 0 : (uint8_t)1 << (i % 8);
        if ((i > 0 && i % 8 == 0) || i == fields_.size() - 1)
        {
            memcpy(bit_map_ptr, &bit_map, 1);
            bit_map_ptr++;
            bit_map = 0;
        }
        buf += fields_[i]->SerializeTo(buf);
        fields_[i]->~Field();
    }
    return buf - origin;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema)
{
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(fields_.empty(), "Non empty field in row.");
    // replace with your code here
    union
    {
        int32_t int_;
        uint32_t uint_;
        float_t float_;
        uint8_t data_[4];
    } tmp;

    uint32_t page_id, row_id, magic_num, map_num;
    char *origin = buf, bit_map = 0, *bit_map_ptr;

    memcpy(tmp.data_, buf, 4);
    magic_num = tmp.uint_;
    buf += sizeof(uint32_t);

    memcpy(tmp.data_, buf, 4);
    page_id = tmp.uint_;
    buf += sizeof(uint32_t);

    memcpy(tmp.data_, buf, 4);
    row_id = tmp.uint_;
    buf += sizeof(uint32_t);

    memcpy(tmp.data_, buf, 4);
    map_num = tmp.uint_;
    buf += sizeof(uint32_t);

    rid_ = RowId(page_id, row_id);
    bit_map_ptr = buf;
    buf += map_num;

    for (int i = 0; i < schema->GetColumnCount(); i++)
    {
        if (i % 8 == 0)
        {
            memcpy(&bit_map, bit_map_ptr, 1);
            bit_map_ptr++;
        }
        fields_.emplace_back(nullptr);
        buf += Field::DeserializeFrom(buf, schema->GetColumn(i)->GetType(), &fields_[i], !(bit_map & (uint8_t)1 << (i % 8)));
    }

    ASSERT(magic_num == ROW_MAGIC_NUM, "Invalid magic num");
    return buf - origin;
}

uint32_t Row::GetSerializedSize(Schema *schema) const
{
    ASSERT(schema != nullptr, "Invalid schema before serialize.");
    ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
    // replace with your code here
    int size = 16 + fields_.size() / 8 + 1;
    for (auto it : fields_)
        size += it->GetSerializedSize();
    return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row)
{
    auto columns = key_schema->GetColumns();
    std::vector<Field> fields;
    uint32_t idx;
    for (auto column : columns)
    {
        schema->GetColumnIndex(column->GetName(), idx);
        fields.emplace_back(*this->GetField(idx));
    }
    key_row = Row(fields);
}

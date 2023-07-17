#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const
{
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto iter : table_meta_pages_)
    {
        MACH_WRITE_TO(table_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
    for (auto iter : index_meta_pages_)
    {
        MACH_WRITE_TO(index_id_t, buf, iter.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, iter.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf)
{
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++)
    {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++)
    {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

uint32_t CatalogMeta::GetSerializedSize() const
{
    return sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + (sizeof(table_id_t) + sizeof(page_id_t) * table_meta_pages_.size()) + (sizeof(index_id_t) + sizeof(page_id_t) * index_meta_pages_.size());
}

CatalogMeta::CatalogMeta() {}

CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager)
{
    if (init)
    {
        catalog_meta_ = CatalogMeta::NewInstance();
        next_table_id_ = next_index_id_ = 0;
    }
    else
    {
        Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(catalog_meta_page->GetData());
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);

        next_table_id_ = catalog_meta_->table_meta_pages_.size() == 0 ? 0 : catalog_meta_->table_meta_pages_.rbegin()->first + 1;
        next_index_id_ = catalog_meta_->index_meta_pages_.size() == 0 ? 0 : catalog_meta_->index_meta_pages_.rbegin()->first + 1;

        for (auto it : catalog_meta_->table_meta_pages_)
        // std::map<table_id_t, page_id_t>
        {
            LoadTable(it.first, it.second);
        }

        for (auto it : catalog_meta_->index_meta_pages_)
        // std::map<index_id_t, page_id_t>
        {
            LoadIndex(it.first, it.second);
        }
    }
}

CatalogManager::~CatalogManager()
{
    // After you finish the code for the CatalogManager section,
    //  you can uncomment the commented code. Otherwise it will affect b+tree test
    FlushCatalogMetaPage();
    for (auto &it : catalog_meta_->table_meta_pages_)
    {
        buffer_pool_manager_->FlushPage(it.second);
    }
    for (auto &it : catalog_meta_->index_meta_pages_)
    {
        buffer_pool_manager_->FlushPage(it.second);
    }

    delete catalog_meta_;
    for (auto iter : tables_)
    {
        delete iter.second;
    }
    for (auto iter : indexes_)
    {
        delete iter.second;
    }
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info)
{
    if (table_names_.find(table_name) != table_names_.end())
        return DB_TABLE_ALREADY_EXIST;

    table_id_t table_id = catalog_meta_->GetNextTableId();
    table_names_[table_name] = table_id;
    page_id_t meta_page_id;
    Page *table_meta_page = buffer_pool_manager_->NewPage(meta_page_id);
    catalog_meta_->table_meta_pages_[table_id] = meta_page_id;

    Schema *schema_copy = Schema::DeepCopySchema(schema);
    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema_copy, txn, log_manager_, lock_manager_);
    TableMetadata *table_meta_data = TableMetadata::Create(table_id, table_name, meta_page_id, schema_copy);
    table_meta_data->SerializeTo(table_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(meta_page_id, true);

    table_info = TableInfo::Create();
    table_info->Init(table_meta_data, table_heap);
    tables_[table_id] = table_info;

    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info)
{
    if (table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;

    table_info = tables_[table_names_[table_name]];
    return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const
{
    if (tables_.empty())
        return DB_TABLE_NOT_EXIST;
    for (auto it : tables_)
        tables.push_back(it.second);
    return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type)
{
    if (table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;

    if (index_names_[table_name].find(index_name) != index_names_[table_name].end())
        return DB_INDEX_ALREADY_EXIST;

    table_id_t table_id = table_names_[table_name];
    TableInfo *table_info = tables_[table_id];

    Schema *schema = table_info->GetSchema();
    std::vector<uint32_t> key_map;
    for (auto key : index_keys)
    {
        uint32_t index;
        if (schema->GetColumnIndex(key, index) == DB_COLUMN_NAME_NOT_EXIST)
            return DB_COLUMN_NAME_NOT_EXIST;
        key_map.push_back(index);
    }

    index_id_t index_id = catalog_meta_->GetNextIndexId();
    index_names_[table_name][index_name] = index_id;

    page_id_t page_id;
    Page *index_meta_page = buffer_pool_manager_->NewPage(page_id);
    catalog_meta_->index_meta_pages_[index_id] = page_id;
    IndexMetadata *index_meta_data = IndexMetadata::Create(index_id, index_name, table_id, key_map);
    index_meta_data->SerializeTo(index_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, true);

    index_info = IndexInfo::Create();
    index_info->Init(index_meta_data, table_info, buffer_pool_manager_);
    indexes_[index_id] = index_info;

    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const
{
    if (table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;

    if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end())
        return DB_INDEX_NOT_FOUND;

    index_id_t index_id = index_names_.at(table_name).at(index_name);
    index_info = indexes_.at(index_id);
    return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const
{
    if (table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;

    auto map = index_names_.at(table_name);
    for (auto it : map)
    {
        index_id_t index_id = it.second;
        IndexInfo *index_info = indexes_.at(index_id);
        indexes.emplace_back(index_info);
    }
    return DB_SUCCESS;
}

// TODO: delete from disk?
dberr_t CatalogManager::DropTable(const string &table_name)
{
    if (table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;

    table_id_t table_id = table_names_[table_name];
    // tables_.at(table_id)->GetTableHeap()->DeleteTable();

    auto idx = index_names_.at(table_name);
    for (auto it : idx)
    {
        DropIndex(table_name, it.first);
    }
    index_names_.erase(table_name);

    free(tables_[table_id]);
    tables_.erase(table_id);
    table_names_.erase(table_name);

    page_id_t table_page_id = catalog_meta_->table_meta_pages_[table_id];
    buffer_pool_manager_->DeletePage(table_page_id);
    catalog_meta_->table_meta_pages_.erase(table_id);

    return DB_SUCCESS;
}

// TODO: delete from disk?
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name)
{
    if (table_names_.find(table_name) == table_names_.end())
        return DB_TABLE_NOT_EXIST;

    if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end())
        return DB_INDEX_NOT_FOUND;

    index_id_t index_id = index_names_.at(table_name).at(index_name);
    indexes_.at(index_id)->GetIndex()->Destroy();
    delete indexes_.at(index_id);
    indexes_.erase(index_id);
    page_id_t index_page_id = catalog_meta_->index_meta_pages_[index_id];
    buffer_pool_manager_->DeletePage(index_page_id);
    catalog_meta_->index_meta_pages_.erase(index_id);
    index_names_.at(table_name).erase(index_name);

    return DB_SUCCESS;
}

dberr_t CatalogManager::FlushCatalogMetaPage() const
{
    Page *meta_page = buffer_pool_manager_->FetchPage(META_PAGE_ID);
    catalog_meta_->SerializeTo(meta_page->GetData());

    buffer_pool_manager_->UnpinPage(META_PAGE_ID, true);
    if (!buffer_pool_manager_->FlushPage(META_PAGE_ID))
        return DB_FAILED;
    return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id)
{
    if (tables_.find(table_id) != tables_.end())
        return DB_TABLE_ALREADY_EXIST;

    // catalog_meta_->table_meta_pages_[table_id] = page_id;

    Page *table_meta_page = buffer_pool_manager_->FetchPage(page_id);
    buffer_pool_manager_->UnpinPage(page_id, false);

    TableMetadata *table_meta_data = nullptr;
    TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta_data);

    table_names_[table_meta_data->GetTableName()] = table_id;

    TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta_data->GetFirstPageId(), table_meta_data->GetSchema(), log_manager_, lock_manager_);
    TableInfo *table_info = TableInfo::Create();
    table_info->Init(table_meta_data, table_heap);
    tables_[table_id] = table_info;

    return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id)
{
    if (indexes_.find(index_id) != indexes_.end())
        return DB_INDEX_ALREADY_EXIST;

    catalog_meta_->index_meta_pages_[index_id] = page_id;

    Page *index_meta_page = buffer_pool_manager_->FetchPage(page_id);
    buffer_pool_manager_->UnpinPage(page_id, false);

    IndexMetadata *index_meta_data = nullptr;
    IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta_data);

    //! table maynot loaded
    TableInfo *table_info = tables_.at(index_meta_data->GetTableId());
    std::string table_name = table_info->GetTableName();
    index_names_[table_name][index_meta_data->GetIndexName()] = index_id;

    IndexInfo *index_info = IndexInfo::Create();
    index_info->Init(index_meta_data, table_info, buffer_pool_manager_);
    indexes_[index_id] = index_info;
    // TODO: load from disk
    return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info)
{
    if (tables_.find(table_id) == tables_.end())
        return DB_TABLE_NOT_EXIST;

    table_info = tables_[table_id];
    return DB_SUCCESS;
}
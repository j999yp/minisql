//
// Created by njz on 2023/1/27.
//

#include "executor/executors/insert_executor.h"

InsertExecutor::InsertExecutor(ExecuteContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init()
{
    std::string table_name = plan_->GetTableName();
    exec_ctx_->GetCatalog()->GetTable(table_name, table_info);
    exec_ctx_->GetCatalog()->GetTableIndexes(table_name, indexes);
    child_executor_->Init();

#ifndef INSERT_NEXT_VERSION
    Row row;
    RowId rid;
    while (child_executor_->Next(&row, &rid))
    {
        bool flag = false;
        std::vector<Row> keys;
        keys.clear();
        for (auto idx : indexes)
        {
            std::vector<Field> key_fields;
            for (auto col : idx->GetIndexKeySchema()->GetColumns())
                key_fields.push_back(*row.GetField(col->GetTableInd()));

            Row key(key_fields);
            keys.push_back(key);
            std::vector<RowId> scan_buffer;
            if (idx->GetIndex()->ScanKey(key, scan_buffer, nullptr) == DB_SUCCESS) // index already exists
            {
                flag = true;
                break;
            }
        }
        if (flag)
            continue;

        table_info->GetTableHeap()->InsertTuple(row, nullptr);
        if (keys.size() != indexes.size())
            throw std::runtime_error("size of both containers should equal.");
        for (int i = 0; i < keys.size(); i++)
        {
            indexes[i]->GetIndex()->InsertEntry(keys[i], row.GetRowId(), nullptr);
        }
        //? python flavor code, available in c++23
        // for (auto const& [index, key] : std::views::zip(indexes, keys))
        // {
        //     index->GetIndex()->InsertEntry(key, row.GetRowId(), nullptr);
        // }
    }
    is_finished = true;
#endif
}

bool InsertExecutor::Next([[maybe_unused]] Row *_row, RowId *_rid)
{
#ifdef INSERT_NEXT_VERSION
    Row row;
    RowId rid;
    if (child_executor_->Next(&row, &rid))
    {
        std::vector<Row> keys{};
        for (auto idx : indexes)
        {
            std::vector<Field> key_fields;
            for (auto col : idx->GetIndexKeySchema()->GetColumns())
                key_fields.push_back(*row.GetField(col->GetTableInd()));

            Row key(key_fields);
            keys.push_back(key);
            std::vector<RowId> scan_buffer;
            if (idx->GetIndex()->ScanKey(key, scan_buffer, nullptr) == DB_SUCCESS) // index already exists
            {
                return false;
            }
        }

        table_info->GetTableHeap()->InsertTuple(row, nullptr);
        if (keys.size() != indexes.size())
            throw std::runtime_error("size of both containers should be equal.");
        for (int i = 0; i < keys.size(); i++)
        {
            indexes[i]->GetIndex()->InsertEntry(keys[i], row.GetRowId(), nullptr);
        }
        return true;
    }
    return false;
#else
    return is_finished;
#endif
}
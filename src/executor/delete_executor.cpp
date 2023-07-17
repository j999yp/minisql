//
// Created by njz on 2023/1/29.
//

#include "executor/executors/delete_executor.h"

DeleteExecutor::DeleteExecutor(ExecuteContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init()
{
    std::string table_name = plan_->GetTableName();
    exec_ctx_->GetCatalog()->GetTable(table_name, table_info);
    exec_ctx_->GetCatalog()->GetTableIndexes(table_name, indexes);
    child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Row *_row, RowId *_rid)
{
    Row row;
    RowId rid;
    if (child_executor_->Next(&row, &rid))
    {
        if (!table_info->GetTableHeap()->MarkDelete(rid, nullptr))
            return false;

        for (auto index : indexes)
        {
            std::vector<Field> fields{};
            for (auto col : index->GetIndexKeySchema()->GetColumns())
            {
                fields.push_back(*row.GetField(col->GetTableInd()));
            }

            Row idx(fields);
            index->GetIndex()->RemoveEntry(idx, rid, nullptr);
        }
        return true;
    }
    return false;
}
//
// Created by njz on 2023/1/30.
//

#include "executor/executors/update_executor.h"

UpdateExecutor::UpdateExecutor(ExecuteContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void UpdateExecutor::Init()
{
    exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info);
    child_executor_->Init();
}

bool UpdateExecutor::Next([[maybe_unused]] Row *row, RowId *rid)
{
    Row old_row;
    RowId old_rid;
    if (child_executor_->Next(&old_row, &old_rid))
    {
        Row new_row = GenerateUpdatedTuple(old_row);
        table_info->GetTableHeap()->UpdateTuple(new_row, old_rid, nullptr);
        RowId new_rid = new_row.GetRowId();

        for (auto index_info : index_info_)
        {
            auto index = index_info->GetIndex();
            std::vector<Field> fields{};
            for (auto col : index_info->GetIndexKeySchema()->GetColumns())
            {
                fields.push_back(*old_row.GetField(col->GetTableInd()));
            }
            Row old_index(fields);
            index->RemoveEntry(old_index, old_rid, nullptr);

            fields.clear();
            for (auto col : index_info->GetIndexKeySchema()->GetColumns())
            {
                fields.push_back(*new_row.GetField(col->GetTableInd()));
            }
            Row new_index(fields);
            index->InsertEntry(new_index, new_rid, nullptr);
        }
        return true;
    }
    return false;
}

Row UpdateExecutor::GenerateUpdatedTuple(const Row &src_row)
{
    std::vector<Field> fields;
    fields.reserve(src_row.GetFieldCount());

    for (int i = 0; i < src_row.GetFieldCount(); i++)
    {
        fields.push_back(*src_row.GetField(i));
    }

    for (auto modify : plan_->GetUpdateAttr())
    {
        Field new_field = modify.second->Evaluate(&src_row);
        fields[modify.first] = new_field;
    }

    return Row(fields);
}
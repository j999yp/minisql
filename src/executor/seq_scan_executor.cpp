//
// Created by njz on 2023/1/17.
//
#include "executor/executors/seq_scan_executor.h"

/**
 * TODO: Student Implement
 */
SeqScanExecutor::SeqScanExecutor(ExecuteContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan) {}

void SeqScanExecutor::Init()
{
    if (exec_ctx_->GetCatalog()->GetTable(plan_->GetTableName(), table_info) == DB_TABLE_NOT_EXIST)
        throw std::runtime_error("no such table");
    table_iter = table_info->GetTableHeap()->Begin(nullptr);
    end = table_info->GetTableHeap()->End();
}

bool SeqScanExecutor::Next(Row *row, RowId *rid)
{
    while (table_iter != end)
    {
        if (plan_->filter_predicate_ == nullptr ||
            plan_->filter_predicate_.get()->Evaluate(table_iter.operator->()).CompareEquals(Field(kTypeInt, 1)))
        {
            vector<Field> output{};
            auto columns = plan_->OutputSchema()->GetColumns();
            Row *tuple = table_iter.operator->();
            for (auto col : columns)
            {
                output.push_back(*tuple->GetField(col->GetTableInd()));
            }
            *row = Row(output);
            row->SetRowId(table_iter.GetRid());
            *rid = RowId(table_iter.GetRid());
            ++table_iter;
            return true;
        }
        else
            ++table_iter;
    }
    return false;
}

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
    table_iter = new TableIterator(table_info->GetTableHeap());
}

bool SeqScanExecutor::Next(Row *row, RowId *rid)
{
    while (*table_iter != TableIterator())
    {
        Row *current_row = table_iter++->operator->();

        if (plan_->filter_predicate_ == nullptr ||
            plan_->filter_predicate_.get()->Evaluate(current_row).CompareEquals(Field(kTypeInt, 1)) == kTrue)
        {
            vector<Field> output{};
            auto columns = plan_->OutputSchema()->GetColumns();
            output.reserve(columns.size());
            for (int idx = 0; auto col : columns)
            {
                uint32_t index;
                table_info->GetSchema()->GetColumnIndex(col->GetName(), index);
                output[idx++] = *current_row->GetField(index);
            }
            row = new Row(output);
            rid = new RowId(current_row->GetRowId());
            return true;
        }
    }
    return false;
}

SeqScanExecutor::~SeqScanExecutor()
{
    delete table_iter;
}

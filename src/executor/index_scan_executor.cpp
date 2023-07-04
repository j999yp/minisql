#include "executor/executors/index_scan_executor.h"
#include <algorithm>
/**
 * TODO: Student Implement
 */
IndexScanExecutor::IndexScanExecutor(ExecuteContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void IndexScanExecutor::Init()
{
    std::string table_name = plan_->GetTableName();
    exec_ctx_->GetCatalog()->GetTable(table_name, table_info);
    Schema *schema = table_info->GetSchema();

    std::map<uint32_t, IndexInfo *> col_idx_map;
    for (auto idx : plan_->indexes_)
    {
        uint32_t tmp;
        if (schema->GetColumnIndex(idx->GetIndexName(), tmp) != DB_SUCCESS)
            throw std::runtime_error("failed to get key index");
        col_idx_map[tmp] = idx;
    }

    std::vector<std::vector<RowId>> tmp{};
    ScanIndex(plan_->GetPredicate(), col_idx_map, tmp);
    res = tmp[0];
    for (int i = 1; i < tmp.size(); i++)
    {
        auto end = set_intersection(res.begin(), res.end(), tmp[i].begin(), tmp[i].end(), res.begin(), [](RowId a, RowId b)
                                    { return a.Get() < b.Get(); });
        res.resize(end - res.begin());
    }
    iter = res.begin();
}

bool IndexScanExecutor::Next(Row *row, RowId *rid)
{
    while (iter != res.end())
    {
        Row current_row(*iter++);
        table_info->GetTableHeap()->GetTuple(&current_row, nullptr);
        if (plan_->need_filter_ == false ||
            plan_->filter_predicate_.get()->Evaluate(&current_row).CompareEquals(Field(kTypeInt, 1)) == kTrue)
        {
            vector<Field> output{};
            auto columns = plan_->OutputSchema()->GetColumns();
            output.reserve(columns.size());
            for (int idx = 0; auto col : columns)
            {
                uint32_t index;
                table_info->GetSchema()->GetColumnIndex(col->GetName(), index);
                output[idx++] = *current_row.GetField(index);
            }
            row = new Row(output);
            rid = new RowId(current_row.GetRowId());
            return true;
        }
    }
    return false;
}

void IndexScanExecutor::ScanIndex(AbstractExpressionRef node, const std::map<uint32_t, IndexInfo *> &map, std::vector<std::vector<RowId>> &res)
{
    if (node->GetType() == ExpressionType::ComparisonExpression
    && node->GetChildAt(0)->GetType() == ExpressionType::ColumnExpression
    && node->GetChildAt(1)->GetType() == ExpressionType::ConstantExpression)
    // leaf node
    {
        auto col_idx = dynamic_pointer_cast<ColumnValueExpression>(node->GetChildAt(0))->GetColIdx();
        if (map.count(col_idx))
        // has index
        {
            std::string operand_ = dynamic_pointer_cast<ComparisonExpression>(node)->GetComparisonType();
            const Field &operator_ = dynamic_pointer_cast<ConstantValueExpression>(node->GetChildAt(1))->val_;
            std::vector<Field> f{Field(operator_)};
            Row r(f);
            std::vector<RowId> tmp{};
            map.at(col_idx)->GetIndex()->ScanKey(r, tmp, nullptr, operand_);
            std::sort(tmp.begin(), tmp.end(),
                      [](RowId a, RowId b)
                      {
                          return a.Get() < b.Get();
                      });
            res.push_back(tmp);
        }
        else if (plan_->need_filter_ != true)
            throw std::runtime_error("need_filter should be true");
    }
    else
    {
        for (int i = 0; i < node->GetChildren().size(); i++)
            ScanIndex(node->GetChildAt(i), map, res);
    }
}

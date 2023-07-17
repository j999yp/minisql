#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "VariadicTable.h"
#include <filesystem>

ExecuteEngine::ExecuteEngine()
{
    char path[] = "./databases";
    DIR *dir;
    if ((dir = opendir(path)) == nullptr)
    {
        mkdir("./databases", 0777);
        dir = opendir(path);
    }
    /** When you have completed all the code for
     *  the test, run it using main.cpp and uncomment
     *  this part of the code.
    struct dirent *stdir;
    while((stdir = readdir(dir)) != nullptr) {
      if( strcmp( stdir->d_name , "." ) == 0 ||
          strcmp( stdir->d_name , "..") == 0 ||
          stdir->d_name[0] == '.')
        continue;
      dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
    }
     **/
    closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan)
{
    switch (plan->GetType())
    {
    // Create a new sequential scan executor
    case PlanType::SeqScan:
    {
        return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan:
    {
        return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update:
    {
        auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
        auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
        return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
        // Create a new delete executor
    case PlanType::Delete:
    {
        auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
        auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
        return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert:
    {
        auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
        auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
        return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values:
    {
        return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
        throw std::logic_error("Unsupported plan type.");
    }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Transaction *txn,
                                   ExecuteContext *exec_ctx)
{
    // Construct the executor for the abstract plan node
    auto executor = CreateExecutor(exec_ctx, plan);

    try
    {
        executor->Init();
        RowId rid{};
        Row row{};
        while (executor->Next(&row, &rid))
        {
            if (result_set != nullptr)
            {
                result_set->push_back(row);
            }
        }
    }
    catch (const exception &ex)
    {
        std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
        if (result_set != nullptr)
        {
            result_set->clear();
        }
        return DB_FAILED;
    }
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast)
{
    if (ast == nullptr)
    {
        return DB_FAILED;
    }
    auto start_time = std::chrono::system_clock::now();
    unique_ptr<ExecuteContext> context(nullptr);
    if (!current_db_.empty())
        context = dbs_[current_db_]->MakeExecuteContext(nullptr);
    switch (ast->type_)
    {
    case kNodeCreateDB:
        return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
        return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
        return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
        return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
        return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
        return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
        return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
        return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
        return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
        return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
        return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
        return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
        return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
        return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
        return ExecuteQuit(ast, context.get());
    default:
        break;
    }
    // Plan the query.
    Planner planner(context.get());
    std::vector<Row> result_set{};
    try
    {
        planner.PlanQuery(ast);
        // Execute the query.
        ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
    }
    catch (const exception &ex)
    {
        std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
        return DB_FAILED;
    }
    auto stop_time = std::chrono::system_clock::now();
    double duration_time =
        double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
    // Return the result set as string.
    std::stringstream ss;
    ResultWriter writer(ss);

    if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan)
    {
        auto schema = planner.plan_->OutputSchema();
        auto num_of_columns = schema->GetColumnCount();
        if (!result_set.empty())
        {
            // find the max width for each column
            vector<int> data_width(num_of_columns, 0);
            for (const auto &row : result_set)
            {
                for (uint32_t i = 0; i < num_of_columns; i++)
                {
                    data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
                }
            }
            int k = 0;
            for (const auto &column : schema->GetColumns())
            {
                data_width[k] = max(data_width[k], int(column->GetName().length()));
                k++;
            }
            // Generate header for the result set.
            writer.Divider(data_width);
            k = 0;
            writer.BeginRow();
            for (const auto &column : schema->GetColumns())
            {
                writer.WriteHeaderCell(column->GetName(), data_width[k++]);
            }
            writer.EndRow();
            writer.Divider(data_width);

            // Transforming result set into strings.
            for (const auto &row : result_set)
            {
                writer.BeginRow();
                for (uint32_t i = 0; i < schema->GetColumnCount(); i++)
                {
                    writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
                }
                writer.EndRow();
            }
            writer.Divider(data_width);
        }
        writer.EndInformation(result_set.size(), duration_time, true);
    }
    else
    {
        writer.EndInformation(result_set.size(), duration_time, false);
    }
    std::cout << writer.stream_.rdbuf();
    return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result)
{
    switch (result)
    {
    case DB_ALREADY_EXIST:
        cout << "Database already exists." << endl;
        break;
    case DB_NOT_EXIST:
        cout << "Database not exists." << endl;
        break;
    case DB_TABLE_ALREADY_EXIST:
        cout << "Table already exists." << endl;
        break;
    case DB_TABLE_NOT_EXIST:
        cout << "Table not exists." << endl;
        break;
    case DB_INDEX_ALREADY_EXIST:
        cout << "Index already exists." << endl;
        break;
    case DB_INDEX_NOT_FOUND:
        cout << "Index not exists." << endl;
        break;
    case DB_COLUMN_NAME_NOT_EXIST:
        cout << "Column not exists." << endl;
        break;
    case DB_KEY_NOT_FOUND:
        cout << "Key not exists." << endl;
        break;
    case DB_QUIT:
        cout << "Bye." << endl;
        break;
    default:
        break;
    }
}
/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
    std::string db_name = ast->child_->val_;
    if (db_name.empty())
    {
        return DB_FAILED;
    }
    if (dbs_.find(db_name) != dbs_.end())
    {
        return DB_ALREADY_EXIST;
    }
    dbs_[db_name] = new DBStorageEngine(db_name);
    std::cout << "Database " << db_name << " created." << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
    std::string db_name = ast->child_->val_;
    if (db_name.empty())
    {
        return DB_FAILED;
    }
    auto it = dbs_.find(db_name);
    if (it == dbs_.end())
    {
        return DB_NOT_EXIST;
    }

    dbs_.erase(db_name);
    std::string path = "./databases/" + db_name;
    remove(path.c_str());

    if (current_db_ == db_name)
        current_db_ = "";

    std::cout << "Database " << db_name << " deleted." << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
    if (dbs_.empty())
    {
        std::cout << "Empty database." << std::endl;
        return DB_SUCCESS;
    }

    VariadicTable<std::string> table({"Database"});
    for (auto it : dbs_)
    {
        table.addRow({it.first});
    }
    table.print(std::cout);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
    std::string db_name = ast->child_->val_;
    if (dbs_.find(db_name) == dbs_.end())
    {
        auto path = std::filesystem::path("./databases/" + db_name);
        if (std::filesystem::exists(path))
            dbs_[db_name] = new DBStorageEngine(db_name, false);
        else
            return DB_NOT_EXIST;
    }
    current_db_ = db_name;
    std::cout << "Current database changed to " << db_name << "." << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
    if (current_db_.empty())
    {
        std::cout << "No database selected." << std::endl;
        return DB_FAILED;
    }
    std::vector<TableInfo *> table_info;
    dbs_[current_db_]->catalog_mgr_->GetTables(table_info);

    VariadicTable<std::string> vt({"Table in " + current_db_});
    for (auto it : table_info)
    {
        vt.addRow({it->GetTableName()});
    }
    vt.print(std::cout);
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
    if (current_db_.empty())
    {
        std::cout << "No database selected." << std::endl;
        return DB_FAILED;
    }

    std::string table_name = ast->child_->val_;
    auto column_definition_list_root = ast->child_->next_;

    std::vector<bool> is_unique;
    std::vector<std::string> col_name;
    std::vector<TypeId> col_type;
    std::vector<int> col_len;
    std::vector<std::string> primary_keys;
    for (auto it = column_definition_list_root->child_; it != nullptr; it = it->next_)
    {
        if (it->type_ == kNodeColumnDefinition)
        {
            auto identifier_node = it->child_;
            auto type_node = identifier_node->next_;
            auto len_node = type_node->child_;
            is_unique.emplace_back(std::string(it->val_ ? it->val_ : "") == "unique");
            col_name.emplace_back(std::string(identifier_node->val_));
            std::string type_name(type_node->val_);
            TypeId type;
            int len;
            if (type_name == "int")
            {
                type = kTypeInt;
                len = 0;
            }
            else if (type_name == "char")
            {
                type = kTypeChar;
                float tmp_len = std::stof(len_node->val_);
                if (tmp_len < 0 || tmp_len != (int)tmp_len)
                {
                    LOG(ERROR) << "Invalid data length.";
                    return DB_FAILED;
                }
                len = (int)tmp_len;
            }
            else if (type_name == "float")
            {
                type = kTypeFloat;
                len = 0;
            }
            else
            {
                LOG(ERROR) << "Invalid type: " << type_name;
                return DB_FAILED;
            }
            col_type.emplace_back(type);
            col_len.emplace_back(len);
        }
        else if (it->type_ == kNodeColumnList)
        {
            if (std::string(it->val_) != "primary keys")
                LOG(WARNING) << "Unknown Column List:" << it->val_;
            for (auto key_iter = it->child_; key_iter != nullptr; key_iter = key_iter->next_)
            {
                primary_keys.emplace_back(std::string(key_iter->val_));
                // all primary keys are unique
                int idx = std::find(col_name.begin(), col_name.end(), key_iter->val_) - col_name.begin();
                is_unique[idx] = true;
            }
        }
        else
        {
            LOG(WARNING) << "Node Type " << it->type_ << "shouldn't appear in CreateTable.";
        }
    }

    // create column
    std::vector<Column *> columns;
    bool is_manage = false;
    for (int i = 0; i < col_name.size(); i++)
    {
        if (col_type[i] == kTypeChar)
        {
            columns.emplace_back(new Column(col_name[i], col_type[i], col_len[i], i, false, is_unique[i]));
            is_manage |= true;
        }
        else
        {
            columns.emplace_back(new Column(col_name[i], col_type[i], i, false, is_unique[i]));
        }
    }

    // schema
    Schema *schema = new Schema(columns, is_manage);
    TableInfo *table_info;
    auto ret = context->GetCatalog()->CreateTable(table_name, schema, nullptr, table_info);
    if (ret != DB_SUCCESS)
        return ret;

    //? create table 时还没有记录，即index为空
    // primary index
    IndexInfo *index_info;
    for (auto key : primary_keys)
    {
        ret = context->GetCatalog()->CreateIndex(table_name, key, {key}, nullptr, index_info, "btree");
        if (ret != DB_SUCCESS)
            return ret;
    }

    // unique index
    for (int i = 0; i < is_unique.size(); i++)
    {
        if (is_unique[i] && std::find(primary_keys.begin(), primary_keys.end(), col_name[i]) == primary_keys.end())
        {
            ret = context->GetCatalog()->CreateIndex(table_name, col_name[i], {col_name[i]}, nullptr, index_info, "btree");
            if (ret != DB_SUCCESS)
                return ret;
        }
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
    if (current_db_.empty())
    {
        std::cout << "No database selected." << std::endl;
        return DB_FAILED;
    }

    std::string table_name(ast->child_->val_);
    auto ret = context->GetCatalog()->DropTable(table_name);
    if (ret != DB_SUCCESS)
    {
        return ret;
    }

    std::vector<IndexInfo *> indexes;
    context->GetCatalog()->GetTableIndexes(table_name, indexes);
    for (auto it : indexes)
    {
        ret = context->GetCatalog()->DropIndex(table_name, it->GetIndexName());
        if (ret != DB_SUCCESS)
        {
            return ret;
        }
    }
    std::cout << "Table " << table_name << " deleted." << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
    if (current_db_.empty())
    {
        std::cout << "No database selected." << std::endl;
        return DB_FAILED;
    }

    std::vector<TableInfo *> table_info;
    dbs_[current_db_]->catalog_mgr_->GetTables(table_info);

    for (auto table_it : table_info)
    {
        std::vector<IndexInfo *> index_info{};
        context->GetCatalog()->GetTableIndexes(table_it->GetTableName(), index_info);
        VariadicTable<std::string> vt({"Index in " + table_it->GetTableName()});
        for (auto index_it : index_info)
        {
            vt.addRow({index_it->GetIndexName()});
        }
        vt.print(std::cout);
    }
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
    if (current_db_.empty())
    {
        std::cout << "No database selected." << std::endl;
        return DB_FAILED;
    }

    std::string index_name(ast->child_->val_);
    std::string table_name(ast->child_->next_->val_);
    std::vector<std::string> cols;
    for (auto it = ast->child_->next_->next_->child_; it != nullptr; it = it->next_)
    {
        cols.emplace_back(std::string(it->val_));
    }
    std::string index_type("btree");
    if (ast->child_->next_->next_->next_ != nullptr)
    {
        index_type = std::string(ast->child_->next_->next_->next_->child_->val_);
    }

    TableInfo *table_info;
    auto ret = context->GetCatalog()->GetTable(table_name, table_info);
    if (ret != DB_SUCCESS)
    {
        return ret;
    }

    IndexInfo *index_info;
    ret = context->GetCatalog()->CreateIndex(table_name, index_name, cols, nullptr, index_info, index_type);
    if (ret != DB_SUCCESS)
    {
        return ret;
    }

    for (auto row = table_info->GetTableHeap()->Begin(nullptr); row != table_info->GetTableHeap()->End(); row++)
    {
        std::vector<Field> fields;
        for (auto col : index_info->GetIndexKeySchema()->GetColumns())
        {
            fields.push_back(*row->GetField(col->GetTableInd()));
        }
        Row idx(fields);
        ret = index_info->GetIndex()->InsertEntry(idx, row.GetRid(), nullptr);
        if (ret != DB_SUCCESS)
        {
            return ret;
        }
    }

    std::cout << "Index " << index_name << " created." << std::endl;
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
    if (current_db_.empty())
    {
        std::cout << "No database selected." << std::endl;
        return DB_FAILED;
    }

    std::string index_name(ast->child_->val_), table_name{};
    std::vector<TableInfo *> table_info;
    auto ret = context->GetCatalog()->GetTables(table_info);
    if (ret != DB_SUCCESS)
        return ret;

    for (auto table : table_info)
    {
        IndexInfo *index_info;
        ret = context->GetCatalog()->GetIndex(table->GetTableName(), index_name, index_info);
        if (ret == DB_SUCCESS)
        {
            table_name = table->GetTableName();
            break;
        }
    }

    if (table_name.empty())
    {
        std::cout << "No matching table found." << std::endl;
        return DB_INDEX_NOT_FOUND;
    }

    context->GetCatalog()->DropIndex(table_name, index_name);
    std::cout << "Index " << index_name << " deleted." << std::endl;
    return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
    return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
    return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
extern "C"
{
    int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
    std::cout << std::filesystem::current_path() << std::endl;
    std::string filename(ast->child_->val_);
    std::ifstream s(filename, std::ios::in);

    char buffer[1024];
    if (!s.is_open())
        return DB_FAILED;

    while (!s.eof())
    {
        memset(buffer, 0, 1024);
        s.getline(buffer, 1024);
        // printf("%s\n", buffer);
        YY_BUFFER_STATE bp = yy_scan_string(buffer);
        if (bp == nullptr)
        {
            LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
            exit(1);
        }
        yy_switch_to_buffer(bp);
        MinisqlParserInit();
        yyparse();
        if (MinisqlParserGetError())
        {
            printf("%s\n", MinisqlParserGetErrorMessage());
        }

        auto result = Execute(MinisqlGetParserRootNode());
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();
        ExecuteInformation(result);
        if (result == DB_QUIT)
        {
            break;
        }
    }
    s.close();
    return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context)
{
#ifdef ENABLE_EXECUTE_DEBUG
    LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
    return DB_QUIT;
}

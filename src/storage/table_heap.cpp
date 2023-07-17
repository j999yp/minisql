#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Transaction *txn)
{
    if (row.GetSerializedSize(schema_) > TablePage::SIZE_MAX_ROW)
        return false;

    TablePage *page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    if (page_ptr == nullptr)
        return false;

    while (!page_ptr->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
    {
        page_id_t next = page_ptr->GetNextPageId();
        buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
        if (next == INVALID_PAGE_ID) [[unlikely]]
        {
            TablePage *new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next));
            if (new_page == nullptr)
                return false;
            new_page->Init(next, page_ptr->GetPageId(), log_manager_, txn);
            page_ptr->SetNextPageId(next);
            page_ptr = new_page;
        }
        else
            page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next));
    }

    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), true);
    return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn)
{
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    // If the page could not be found, then abort the transaction.
    if (page == nullptr)
    {
        return false;
    }
    // Otherwise, mark the tuple as deleted.
    page->WLatch();
    page->MarkDelete(rid, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn)
{
    Row old_row(rid);
    TablePage *page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page_ptr == nullptr)
        return false;
    int ret = page_ptr->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);

    bool res = true, is_dirty = false;

    switch (ret)
    {
    case TablePage::ret::INVALID_SLOT:
    case TablePage::ret::ALREADY_DELETED:
        res = false;
        break;
    case TablePage::ret::NOT_ENOUGH_SPACE:
        res &= MarkDelete(rid, txn);
        res &= InsertTuple((Row &)row, txn);
    default: // ret == OK
        is_dirty = true;
    }

    buffer_pool_manager_->UnpinPage(rid.GetPageId(), is_dirty);
    return res;
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn)
{
    // Step1: Find the page which contains the tuple.
    // Step2: Delete the tuple from the page.
    TablePage *page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page_ptr == nullptr)
        return;
    page_ptr->ApplyDelete(rid, txn, log_manager_);
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn)
{
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    // Rollback to delete.
    page->WLatch();
    page->RollbackDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

bool TableHeap::GetTuple(Row *row, Transaction *txn)
{
    TablePage *page_ptr = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    if (page_ptr == nullptr)
        return false;
    bool res = page_ptr->GetTuple(row, schema_, txn, lock_manager_);
    buffer_pool_manager_->UnpinPage(page_ptr->GetPageId(), false);
    return res;
}

void TableHeap::DeleteTable(page_id_t page_id)
{
    if (page_id != INVALID_PAGE_ID)
    {
        auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id)); // 删除table_heap
        if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
            DeleteTable(temp_table_page->GetNextPageId());
        buffer_pool_manager_->UnpinPage(page_id, false);
        buffer_pool_manager_->DeletePage(page_id);
    }
    else
    {
        DeleteTable(first_page_id_);
    }
}

TableIterator TableHeap::Begin(Transaction *txn)
{
    Page *page_ptr = buffer_pool_manager_->FetchPage(first_page_id_);
    if (page_ptr == nullptr)
    {
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        return End();
    }
    TablePage *table_page = reinterpret_cast<TablePage *>(page_ptr->GetData());
    RowId first_rid;
    if (table_page->GetFirstTupleRid(&first_rid))
    {
        // Row tst;
        // table_page->GetTuple(&tst,schema_,nullptr,nullptr);
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        return TableIterator(this, first_rid);
    }
    return End();
}

TableIterator TableHeap::End()
{
    RowId rid(INVALID_PAGE_ID, 0);
    return TableIterator(this, rid);
}

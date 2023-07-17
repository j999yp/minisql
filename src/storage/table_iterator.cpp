#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator()
{
}

TableIterator::~TableIterator()
{
    delete row;
}

TableIterator::TableIterator(TableHeap *heap)
{
    tables = heap;
    rid = RowId(heap->first_page_id_, 0);
}

TableIterator::TableIterator(TableHeap *heap, RowId &rid)
{
    tables = heap;
    this->rid = rid;
}

TableIterator::TableIterator(const TableIterator &other)
{
    tables = other.tables;
    rid = other.rid;
}

bool TableIterator::operator==(const TableIterator &itr) const
{
    return rid == itr.rid && tables == itr.tables;
}

bool TableIterator::operator!=(const TableIterator &itr) const
{
    return !(*this == itr);
}

const Row &TableIterator::operator*()
{
    ASSERT(*this != tables->End(), "OOB error");

    row->CleanRow();
    row->SetRowId(rid);
    tables->GetTuple(row, nullptr);
    return *row;
}

Row *TableIterator::operator->()
{
    ASSERT(*this != tables->End(), "OOB error");

    row->CleanRow();
    row->SetRowId(rid);
    tables->GetTuple(row, nullptr);
    return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept
{
    this->rid = itr.rid;
    this->tables = itr.tables;
    return *this;
    // TableIterator tmp(itr);
    // return tmp;
}

// ++iter
TableIterator &TableIterator::operator++()
{
    FindNextRow(rid);
    return *this;
}

// iter++
TableIterator TableIterator::operator++(int)
{
    TableIterator tmp(*this);
    FindNextRow(rid);
    return tmp;
}

void TableIterator::FindNextRow(RowId &row_id)
{
    RowId next;
    TablePage *current_page = reinterpret_cast<TablePage *>(tables->buffer_pool_manager_->FetchPage(row_id.GetPageId())->GetData());
    tables->buffer_pool_manager_->UnpinPage(current_page->GetPageId(), false);
    if (current_page->GetNextTupleRid(row_id, &next))
        row_id = next;
    // current page has no more rows
    else
    {
        page_id_t next_page_id = current_page->GetNextPageId();
        if (next_page_id == INVALID_PAGE_ID)
        {
            row_id = INVALID_ROWID;
        }
        else
            row_id.Set(next_page_id, 0);
    }
}

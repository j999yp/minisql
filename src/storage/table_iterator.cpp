#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

TableIterator::TableIterator()
{
}

TableIterator::TableIterator(TableHeap *heap)
{
    table = heap;
    rid = RowId(heap->first_page_id_, 0);
}

TableIterator::TableIterator(TableHeap *heap, RowId &rid)
{
    table = heap;
    rid = rid;
}

TableIterator::TableIterator(const TableIterator &other)
{
    table = other.table;
    rid = other.rid;
}

TableIterator::~TableIterator()
{
    delete row;
}

bool TableIterator::operator==(const TableIterator &itr) const
{
    return rid == itr.rid && table == itr.table;
}

bool TableIterator::operator!=(const TableIterator &itr) const
{
    return !(*this == itr);
}

const Row &TableIterator::operator*()
{
    ASSERT(*this != table->End(), "OOB error");

    table->GetTuple(row, nullptr);
    return *row;
}

Row *TableIterator::operator->()
{
    ASSERT(*this != table->End(), "OOB error");

    table->GetTuple(row, nullptr);
    return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept
{
    TableIterator tmp(itr);
    return tmp;
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
    return TableIterator(tmp);
}

void TableIterator::FindNextRow(RowId &row_id)
{
    RowId next(row_id);
    TablePage *page_ptr = reinterpret_cast<TablePage *>(table->buffer_pool_manager_->FetchPage(row_id.GetPageId()));
    // current page has no more rows
    if (!page_ptr->GetNextTupleRid(row_id, &next))
    {
        page_id_t next = page_ptr->GetNextPageId();
        if (next != INVALID_PAGE_ID)
            row_id.Set(next, 0);
        else
            row_id = INVALID_ROWID;
    }
}

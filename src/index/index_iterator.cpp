#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm)
{
    page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
}

IndexIterator::~IndexIterator()
{
    if (current_page_id != INVALID_PAGE_ID)
        buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*()
{
    // ASSERT(false, "Not implemented yet.");
    ASSERT(page != nullptr, "Invalid access");
    return page->GetItem(item_index);
}

IndexIterator &IndexIterator::operator++()
{
    // ASSERT(false, "Not implemented yet.");
    if (++item_index == page->GetSize())
    {
        page_id_t next_page_id = page->GetNextPageId();
        buffer_pool_manager->UnpinPage(current_page_id, false);
        if (next_page_id == INVALID_PAGE_ID) // no more pages
        {
            // page = nullptr;
            current_page_id = INVALID_PAGE_ID;
            item_index = 0;
        }
        else
        {
            page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(next_page_id)->GetData());
            current_page_id = next_page_id;
            item_index = 0;
        }
    }
}

bool IndexIterator::operator==(const IndexIterator &itr) const
{
    return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const
{
    return !(*this == itr);
}
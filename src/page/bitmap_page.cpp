#include "page/bitmap_page.h"

#include "glog/logging.h"
#include <bits/stdc++.h>

template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset)
{
    // used up pages
    if (page_allocated_ == GetMaxSupportedSize())
        return false;

    page_offset = next_free_page_;

    bytes[next_free_page_ / 8] |= (uint8_t)0b1 << (next_free_page_ % 8);
    page_allocated_++;

    // find first free bit
    for (int i = 0; i < MAX_CHARS; i++)
    {
        if (bytes[i] != 0xff)
        {
            next_free_page_ = i * 8 + ffs(~bytes[i]) - 1;
            break;
        }
    }
    return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset)
{
    if (IsPageFree(page_offset))
        return false;
    bytes[page_offset / 8] &= ~((uint8_t)0b1 << (page_offset % 8));
    page_allocated_--;
    next_free_page_ = page_offset;
    return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const
{
    return IsPageFreeLow(page_offset / 8, page_offset % 8);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const
{
    return !(bytes[byte_index] & (0b1 << bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
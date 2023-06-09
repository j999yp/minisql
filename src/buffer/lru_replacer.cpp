#include "buffer/lru_replacer.h"
#include <algorithm>

LRUReplacer::LRUReplacer(size_t num_pages)
{
    capacity_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id)
{
    if (lru_list_->size() == 0)
    {
        *frame_id = -1; //? instruction says return nullptr
        return false;
    }
    *frame_id = lru_list_->back();
    lru_list_->pop_back();
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id)
{
    lru_list_->remove(frame_id);
}

void LRUReplacer::Unpin(frame_id_t frame_id)
{
    if (lru_list_->size() < capacity_ && std::find(lru_list_->begin(), lru_list_->end(), frame_id) == lru_list_->end())
        lru_list_->push_front(frame_id);
}

size_t LRUReplacer::Size()
{
    return lru_list_->size();
}
#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
    SetPageType(IndexPageType::INTERNAL_PAGE);
    SetSize(1); // INVALD PAGE
    SetPageId(page_id);
    SetParentPageId(parent_id);
    SetKeySize(key_size);
    SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index)
{
    return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key)
{
    memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const
{
    return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value)
{
    *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const
{
    for (int i = 0; i < GetSize(); ++i)
    {
        if (ValueAt(i) == value)
            return i;
    }
    return -1;
}

void *InternalPage::PairPtrAt(int index)
{
    return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num)
{
    memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM)
{
    int left = 0, right = GetSize() - 1;
    if (right == 0)
        return ValueAt(0);
    while (right - left > 1)
    {
        int mid = (left + right) / 2;
        int res = KM.CompareKeys(key, KeyAt(mid));
        if (res > 0)
            left = mid;
        else if (res < 0)
            right = mid;
        else
            return ValueAt(mid);
    }
    return KM.CompareKeys(key, KeyAt(right)) < 0 ? ValueAt(left) : ValueAt(right);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value)
{
    SetValueAt(0, old_value);
    SetKeyAt(1, new_key);
    SetValueAt(1, new_value);
    SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value)
{
    int index = ValueIndex(old_value) + 1;
    //! may cause overflow
    std::move_backward(data_ + index * pair_size, data_ + GetSize() * pair_size, data_ + (GetSize() + 1) * pair_size);
    SetKeyAt(index, new_key);
    SetValueAt(index, new_value);
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager)
{
    int total = GetSize(), idx = GetMinSize();
    if (total > idx)
    {
        recipient->CopyNFrom(data_ + idx * pair_size, total - idx, buffer_pool_manager);
        SetSize(idx);
    }
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager)
{
    memcpy(data_ + GetSize() * pair_size, src, size * pair_size); //! append to data_, may cause overflow
    int idx = GetSize();
    for (int i = 0; i < size; i++)
    {
        page_id_t page_id = ValueAt(idx + i);
        BPlusTreePage *page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(page_id)->GetData());
        page_ptr->SetParentPageId(GetPageId());
        buffer_pool_manager->UnpinPage(page_id, true);
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index)
{
    std::move(data_ + (index + 1) * pair_size, data_ + GetSize() * pair_size, data_ + index * pair_size);
    IncreaseSize(-1);
    memset(data_ + GetSize() * pair_size, 0, pair_size); // cleaning rubbish data
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
//? whether INVALID_KEY is included
page_id_t InternalPage::RemoveAndReturnOnlyChild()
{
    SetSize(0);
    return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager)
{
    SetKeyAt(0, middle_key);
    recipient->CopyNFrom(data_, GetSize(), buffer_pool_manager);
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager)
{
    SetKeyAt(0, middle_key);
    recipient->CopyNFrom(data_, 1, buffer_pool_manager);
    Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager)
{
    SetValueAt(GetSize(), value);
    SetKeyAt(GetSize(), key);
    IncreaseSize(1);
    BPlusTreePage *page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
    page_ptr->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
// TODO: update parent entry
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager)
{
    recipient->SetKeyAt(0, middle_key);
    recipient->CopyFirstFrom(ValueAt(GetSize() - 1), buffer_pool_manager); //? child page updated in this function
    IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
//? first key is updated somewhere else
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager)
{
    std::move_backward(data_, data_ + GetSize() * pair_size, data_ + (GetSize() + 1) * pair_size);
    SetValueAt(0, value);
    BPlusTreePage *page_ptr = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager->FetchPage(value)->GetData());
    page_ptr->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
    IncreaseSize(1);
}
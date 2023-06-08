#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file)
{
    std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    // directory or file does not exist
    if (!db_io_.is_open())
    {
        db_io_.clear();
        // create a new file
        std::filesystem::path p = db_file;
        if (p.has_parent_path())
            std::filesystem::create_directories(p.parent_path());
        db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
        db_io_.close();
        // reopen with original mode
        db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
        if (!db_io_.is_open())
        {
            throw std::exception();
        }
    }
    ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close()
{
    std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
    if (!closed)
    {
        db_io_.close();
        closed = true;
    }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data)
{
    ASSERT(logical_page_id >= 0, "Invalid page id.");
    ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data)
{
    ASSERT(logical_page_id >= 0, "Invalid page id.");
    WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage()
{
    DiskFileMetaPage *meta_data = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
    if (meta_data->GetAllocatedPages() == MAX_VALID_PAGE_ID)
        return INVALID_PAGE_ID;

    int i = 0;
    for (; i < meta_data->num_extents_; i++)
        if (meta_data->extent_used_page_[i] < BITMAP_SIZE)
            break;

    uint32_t offset = 0;
    BitmapPage<PAGE_SIZE> *page = new BitmapPage<PAGE_SIZE>();

    meta_data->num_allocated_pages_++;
    meta_data->extent_used_page_[i]++;

    if (i < meta_data->num_extents_)
    {
        ReadPhysicalPage(i * (BITMAP_SIZE + 1) + 1, (char *)page);
    }
    else
    {
        meta_data->num_extents_++;
    }
    page->AllocatePage(offset);
    WritePhysicalPage(i * (BITMAP_SIZE + 1) + 1, (const char *)page);
    delete page;
    return i * BITMAP_SIZE + offset;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id)
{
    DiskFileMetaPage *meta_data = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
    char buffer[PAGE_SIZE] = {0};
    BitmapPage<PAGE_SIZE> *page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buffer);

    ReadPhysicalPage(MapPageId(logical_page_id), buffer);
    bool res = page->DeAllocatePage(logical_page_id % BITMAP_SIZE);
    meta_data->num_allocated_pages_ -= res ? 1 : 0;
    meta_data->extent_used_page_[logical_page_id / BITMAP_SIZE] -= res ? 1 : 0;
    if (meta_data->extent_used_page_[logical_page_id / BITMAP_SIZE] == 0)
        meta_data->num_extents_--;
    WritePhysicalPage(MapPageId(logical_page_id), buffer);
}

bool DiskManager::IsPageFree(page_id_t logical_page_id)
{
    DiskFileMetaPage *meta_data = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
    char buffer[PAGE_SIZE] = {0};
    ReadPhysicalPage(MapPageId(logical_page_id), buffer);
    BitmapPage<PAGE_SIZE> *page = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(buffer);
    return page->IsPageFree(logical_page_id % BITMAP_SIZE);
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id)
{
    return logical_page_id / BITMAP_SIZE * (1 + BITMAP_SIZE) + 1;
}

int DiskManager::GetFileSize(const std::string &file_name)
{
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data)
{
    int offset = physical_page_id * PAGE_SIZE;
    // check if read beyond file length
    if (offset >= GetFileSize(file_name_))
    {
#ifdef ENABLE_BPM_DEBUG
        LOG(INFO) << "Read less than a page" << std::endl;
#endif
        memset(page_data, 0, PAGE_SIZE);
    }
    else
    {
        // set read cursor to offset
        db_io_.seekp(offset);
        db_io_.read(page_data, PAGE_SIZE);
        // if file ends before reading PAGE_SIZE
        int read_count = db_io_.gcount();
        if (read_count < PAGE_SIZE)
        {
#ifdef ENABLE_BPM_DEBUG
            LOG(INFO) << "Read less than a page" << std::endl;
#endif
            memset(page_data + read_count, 0, PAGE_SIZE - read_count);
        }
    }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data)
{
    size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
    // set write cursor to offset
    db_io_.seekp(offset);
    db_io_.write(page_data, PAGE_SIZE);
    // check for I/O error
    if (db_io_.bad())
    {
        LOG(ERROR) << "I/O error while writing";
        return;
    }
    // needs to flush to keep disk file in sync
    db_io_.flush();
}
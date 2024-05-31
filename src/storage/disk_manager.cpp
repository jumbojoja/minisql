#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Assign a free page from disk and return the logical page number of the free page
 * 2024.05.30 Liang 我尝试重构一下这个函数，跑了相关的测试点，都过了，但是正确性还是有待考证
 */
page_id_t DiskManager::AllocatePage() {
  char buf[PAGE_SIZE];    /*页缓冲区*/
  DiskFileMetaPage *meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  /*元数据页记录了分配了多少个数据页，超过了最大数量就分配不了了*/
  if(meta_page->GetAllocatedPages() >= MAX_VALID_PAGE_ID){
    //this disk is full;
    return INVALID_PAGE_ID;
  }
  uint32_t i = 0;
  for(; i < MAX_VALID_PAGE_ID/BitmapPage<PAGE_SIZE>::GetMaxSupportedSize(); i++){
    /*循环条件是当前第i个分区还在最大分区数中*/
    if(meta_page->GetExtentUsedPage(i) < BitmapPage<PAGE_SIZE>::GetMaxSupportedSize()){
      meta_page->num_allocated_pages_++;
      meta_page->extent_used_page_[i]++;
      if(meta_page->extent_used_page_[i] == 1){
        /*说明没有数据页，这个分区事实上是空的，这里等于1是因为前面的++,元数据应该是0*/
        meta_page->num_extents_++;
      }
      /*这里计算出位图页的物理页号，然后读入位图页*/
      page_id_t physical_page_id = i*(BITMAP_SIZE+1)+1;
      ReadPhysicalPage(physical_page_id, buf);
      BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buf);  /*和位图页相关的实现都要加这个转换*/
      uint32_t page_offset;
      if(!bitmap->AllocatePage(page_offset)){ /*使用位图页来分配一个数据页*/
        LOG(ERROR) << "allocate page faild" << std::endl;
      }
      WritePhysicalPage(physical_page_id, buf);
      return BITMAP_SIZE*i + page_offset; /*返回逻辑页号*/
    }
  }
  /*上面是往已经有的分区里面找，结果没找到空闲的，于是执行到这里，新开了个分区*/
  // page_id_t physical_page_id = i*(BITMAP_SIZE+1)+1; /*指向位图页物理页号*/
  // meta_page->num_allocated_pages_++;
  // meta_page->extent_used_page_[i] = 1;
  // meta_page->num_extents_++;
  // physical_page_id = i*(BITMAP_SIZE+1)+1; /*这里似乎重复赋值了，可能没必要*/
  // ReadPhysicalPage(physical_page_id, buf);
  // BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buf);
  // uint32_t page_offset;
  // if(!bitmap->AllocatePage(page_offset)){
  //   LOG(ERROR) << "allocate page faild" << std::endl;
  // }
  // WritePhysicalPage(physical_page_id, buf);
  // return BITMAP_SIZE*i + page_offset;

  //ASSERT(false, "Not implemented yet.");
  /*重构之后，执行到这里说明无法分配*/
  return INVALID_PAGE_ID;
}

/**
 * TODO: Release the physical page corresponding to the logical page number on the disk
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  char buf[PAGE_SIZE];
  /*标准开头，计算位图页物理页号，读入位图页，转换*/
  page_id_t physical_page_id = (logical_page_id/BITMAP_SIZE)*(BITMAP_SIZE+1)+1;
  ReadPhysicalPage(physical_page_id, buf);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buf);
  /*计算这个数据页在位图页中的bit号*/
  uint32_t page_offset = MapPageId(logical_page_id)-physical_page_id-1;
  if(bitmap->DeAllocatePage(page_offset)){
    /*说明deallocate操作成功了，然后做元数据页的更改*/
    DiskFileMetaPage* meta_page = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
    meta_page->num_allocated_pages_--;
    if(!(--meta_page->extent_used_page_[logical_page_id/BITMAP_SIZE])){
      meta_page->num_extents_--;
    }
    WritePhysicalPage(physical_page_id, buf);
  }else{
    //LOG(WARNING) << "NO page to deallocate" << std::endl;
  }
  
  //ASSERT(false, "Not implemented yet.");
}

/**
 * TODO: Check whether the data page corresponding to the logical page number is idle
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char buf[PAGE_SIZE];
  /*这里的physical_page_id指的是相应逻辑页号对应的分块当中的位图页的物理页号
   * 一个分块=一个位图页+bitmap_size*数据页
   */
  page_id_t physical_page_id = (logical_page_id/BITMAP_SIZE)*(BITMAP_SIZE+1)+1;
  /*读入位图页*/
  ReadPhysicalPage(physical_page_id, buf);
  BitmapPage<PAGE_SIZE> *bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buf);
  /*下面这个算式用来计算逻辑页号指向的数据页在位图页中是第几个bit*/
  uint32_t page_offset = MapPageId(logical_page_id)-physical_page_id-1;
  return bitmap->IsPageFree(page_offset);
  //return false;
}

/**
 * TODO: In a private member of the DiskManager class, this function can be used to convert a logical page number to a physical page number
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  return logical_page_id/BITMAP_SIZE + 2 + logical_page_id;
  /*这里的计算可以这样理解：全部的页可以分为三种 ：元数据页 位图页和数据页
   *对于一个逻辑页号为x的数据页，其前面的元数据页有1张，位图页有x/BITMAP_SIZE+1张，最后再加上自己的逻辑页号即是物理页号*/
  //return 0;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

/*这俩函数一个读一个写，都用的C++的函数，很好理解*/
void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      /*读完把后面补零*/
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}
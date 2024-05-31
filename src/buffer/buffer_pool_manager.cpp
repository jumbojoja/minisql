#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Obtain the corresponding data page based on the logical page number
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  /*上面的流程事实上已经讲的很清楚了，有一个关键点提一下
   *这边的FetchPage和FindPage不一样，除非这个页号是Invalid或者超出范围，它都能被找到，只是是在内存中找到还是在磁盘中找到的问题
   *page_table是我们给页做的一个缓存表，如果缓存表里面能找到就用它，找不到只好到下一级去找
   *找到页之后，我们要把这个页放到page_table中。但是现在这个表满了，要换谁呢？看看free_list和replacer来决定
   *表被换掉事实上就是从缓存到了磁盘里面。如果不dirty不用写也没问题，如果dirty就要写了*/
  if(page_id == INVALID_PAGE_ID){
    LOG(WARNING)<<"invalid page id!"<<std::endl;
    return nullptr;
  }
  frame_id_t frame_id_new;
  if(page_table_.find(page_id) == page_table_.end()){
    if(free_list_.size() == 0 && replacer_->Size() == 0){
      return nullptr;
    }
    if(free_list_.size() != 0){
      frame_id_new = free_list_.front();
      free_list_.pop_front();
    }else{
      if(!replacer_->Victim(&frame_id_new)){
        LOG(WARNING)<<"Unkown mistake" << std::endl;
      }
    }
    if(pages_[frame_id_new].is_dirty_){
      disk_manager_->WritePage(pages_[frame_id_new].page_id_, pages_[frame_id_new].data_);
      pages_[frame_id_new].is_dirty_ = false;
    }
    page_table_.erase(pages_[frame_id_new].page_id_);
    page_table_.emplace(page_id, frame_id_new);
    disk_manager_->ReadPage(page_id, pages_[frame_id_new].data_);
    pages_[frame_id_new].is_dirty_ = false;
    pages_[frame_id_new].pin_count_ = 1;
    pages_[frame_id_new].page_id_ = page_id;
    return &pages_[frame_id_new];
  }
  auto i = page_table_.find(page_id);
  Page *page = &pages_[i->second];
  replacer_->Pin(i->second);
  page->pin_count_++;
  return page;

}

/**
 * TODO: Assign a new data page and return the logical page number in the page_id
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  int flag = 1;
  frame_id_t frame_id_new;
  if(free_list_.size() == 0 && replacer_->Size() == 0){
    return nullptr;
  }
  if(free_list_.size() != 0){
    frame_id_new = free_list_.front();
    free_list_.pop_front();
  }else{
    if(!replacer_->Victim(&frame_id_new)){
      LOG(WARNING)<<"Unkown mistake"<<std::endl;
    }
    flag = 0;
  }
  Page *page = &pages_[frame_id_new];
  if((page->is_dirty_) && (!flag)){
    disk_manager_->WritePage(page->page_id_, page->data_);
    page->is_dirty_ = false;
  }
  if(!flag){
    page_table_.erase(page->page_id_);
  }
  page->pin_count_ = 1;
  /*这里最不好理解。事实上，一个页的数据都在磁盘里面，AllocatePage做的事情是在磁盘里给分配一个页的空间
   *但是，在内存当中也有页的数据，就是这里的page->data_
   *当我们写的时候，是把page->data_写到磁盘分配的空间里面
   *当我们读的时候，page->data_会去拿分配给该页的磁盘空间里的数据*/
  page->ResetMemory();
  page_id = AllocatePage();
  page->page_id_ = page_id;
  page_table_.emplace(page_id, frame_id_new);
  return page;
  
  //return nullptr;
}

/**
 * TODO: Release a data page
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  if(page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()){
    return true;
  }
  auto ite = page_table_.find(page_id);
  if(pages_[ite->second].pin_count_ != 0){
    LOG(WARNING) << "Someone is using the page" << std::endl;
    return false;
  }
  if(pages_[ite->second].is_dirty_){
    FlushPage(pages_[ite->second].page_id_);
  }
  DeallocatePage(page_id);
  pages_[ite->second].page_id_ = INVALID_PAGE_ID;
  pages_[ite->second].ResetMemory();
  page_table_.erase(page_id);
  free_list_.push_back(ite->second);
  return true;

}

/**
 * TODO: Unpin a data page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  /*在本线程内将一个页解除Pin，实际上做的是将其pin数减去1，如果到0了直接unpin
   * 单线程情况下没有太多顾虑
   */
  if(page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()){
    return false;
  }
  auto ite = page_table_.find(page_id);
  if(pages_[ite->second].pin_count_ == 0){
    LOG(WARNING) << "This page is already unpin" << std::endl;
    return false;
  }
  pages_[ite->second].pin_count_--;
  if(pages_[ite->second].pin_count_==0){
    replacer_->Unpin(ite->second);
  }
  if(is_dirty){
    pages_[ite->second].is_dirty_ = is_dirty;
  }
  return true;
  
  //return false;
}

/**
 * TODO: Dump the data page to disk
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  if(page_id == INVALID_PAGE_ID || page_table_.find(page_id) == page_table_.end()){
    return false;
  }
  /*无论是否被pin，这个页面都会被flush*/
  auto ite = page_table_.find(page_id);
  disk_manager_->WritePage(page_id, pages_[ite->second].data_);
  pages_[ite->second].is_dirty_ = false;
  return true;
  
  //return false;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}
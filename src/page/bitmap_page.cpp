#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  /*这里的8实际上就是一个char的bit数*/
  if(page_allocated_ == 8*MAX_CHARS)  //no page available
    return false;
  else{
    bytes[next_free_page_/8] |= (1<<(7-(next_free_page_%8))); /*标记该页借出*/
    page_allocated_++;
    page_offset = next_free_page_;
    next_free_page_ = 8*MAX_CHARS;
    for(uint32_t i = (page_offset+1); i < 8*MAX_CHARS; i++){
      if((bytes[i/8]&(1<<(7-(i%8)))) == 0){
        /*循环查找下一个空闲页*/
        next_free_page_ = i;
        break;
      }
    }
    return true;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(IsPageFree(page_offset)) //this page is already free, we don't need to free it again
    return false;
  else{
    bytes[page_offset/8] &= ~(1<<(7-(page_offset%8)));  /*这里写的很高大上，其实就是把这个数据页的bit置0*/
    page_allocated_--;
    if(next_free_page_ > page_offset) /*还了一个编号小于当前next_free_page的，就要更新，否则不需要更新*/
      next_free_page_ = page_offset;
    return true;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset/8;
  uint8_t bit_index = page_offset%8;
  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  /*每一个bit都记录了一个数据页的分配情况，0表示未分配，1表示已经分配*/
  if(bytes[byte_index]&(1<<(7-bit_index)))
    return false;
  else
    return true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;
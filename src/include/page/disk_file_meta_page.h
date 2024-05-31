#ifndef MINISQL_DISK_FILE_META_PAGE_H
#define MINISQL_DISK_FILE_META_PAGE_H

#include <cstdint>

#include "page/bitmap_page.h"

/*这个常量表达式指的是一个元数据页最大能管多少个数据页
 *page_size-8是因为一个元数据页要记录两个32位的无符号整型，一个是已分配的页数，一个是分区的数量
 *上述结果除以4，可以计算出可以管的最大分区数量，因为元数据页中每个分区使用一个32位整型来记录当前分区使用页数，即4个字节
 *最后把结果乘以每个分区的最大数据页数量即是一个元数据页最大能管的数据页数量*/
static constexpr page_id_t MAX_VALID_PAGE_ID = (PAGE_SIZE - 8) / 4 * BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

class DiskFileMetaPage {
 public:
  uint32_t GetExtentNums() { return num_extents_; }

  uint32_t GetAllocatedPages() { return num_allocated_pages_; }

  uint32_t GetExtentUsedPage(uint32_t extent_id) {
    if (extent_id >= num_extents_) {
      return 0;
    }
    return extent_used_page_[extent_id];
  }

 public:
  uint32_t num_allocated_pages_{0};
  uint32_t num_extents_{0};  // each extent consists with a bit map and BIT_MAP_SIZE pages
  uint32_t extent_used_page_[0];//记录每个分区已经使用的页数
};

#endif  // MINISQL_DISK_FILE_META_PAGE_H

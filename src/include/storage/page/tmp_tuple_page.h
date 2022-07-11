#pragma once

#include "storage/page/page.h"
#include "storage/table/tmp_tuple.h"
#include "storage/table/tuple.h"

namespace bustub {

// To pass the test cases for this class, you must follow the existing TmpTuplePage format and implement the
// existing functions exactly as they are! It may be helpful to look at TablePage.
// Remember that this task is optional, you get full credit if you finish the next task.

/**
 * TmpTuplePage format:
 *
 * Sizes are in bytes.
 * | PageId (4) | LSN (4) | FreeSpace (4) | (free space) | TupleSize2 | TupleData2 | TupleSize1 | TupleData1 |
 *                                                       ^
 *                                                       free space pointer
 * We choose this format because DeserializeExpression expects to read Size followed by Data.
 */
class TmpTuplePage : public Page {
 public:
  void Init(page_id_t page_id, uint32_t page_size) {
    memcpy(GetData(), &page_id, sizeof(page_id));
    SetFreeSpacePointer(page_size);
  }

  /** @return the page ID of this temp table page */
  page_id_t GetTablePageId() { return *reinterpret_cast<page_id_t *>(GetData()); }

  bool Insert(const Tuple &tuple, TmpTuple *out) {
    // determine whether there is enough space to insert tuple
    if (GetFreeSpaceRemaining() < tuple.size_ + SIZE_TUPLE) {
      return false;
    }

    // insert tuple and its size
    SetFreeSpacePointer(GetFreeSpacePointer() - tuple.size_);
    memcpy(GetData() + GetFreeSpacePointer(), tuple.data_, tuple.size_);
    SetFreeSpacePointer(GetFreeSpacePointer() - SIZE_TUPLE);
    memcpy(GetData() + GetFreeSpacePointer(), &tuple.size_, SIZE_TUPLE);
    out->SetPageId(GetPageId());
    out->SetOffset(GetFreeSpacePointer());
    return true;
  }

 private:
  static_assert(sizeof(page_id_t) == 4);

  static constexpr size_t SIZE_TABLE_PAGE_HEADER = 12;
  static constexpr size_t SIZE_TUPLE = 4;
  static constexpr size_t OFFSET_FREE_SPACE = 8;

  /** @return pointer to the end of the current free space, see header comment */
  uint32_t GetFreeSpacePointer() { return *reinterpret_cast<uint32_t *>(GetData() + OFFSET_FREE_SPACE); }

  /** set the pointer of the end of current free space.
   * @param free_space_ptr the pointer relative to data_
   */
  void SetFreeSpacePointer(uint32_t free_space_ptr) {
    memcpy(GetData() + OFFSET_FREE_SPACE, &free_space_ptr, sizeof(uint32_t));
  }

  /** @return the size of free space */
  uint32_t GetFreeSpaceRemaining() { return GetFreeSpacePointer() - SIZE_TABLE_PAGE_HEADER; }
};

}  // namespace bustub

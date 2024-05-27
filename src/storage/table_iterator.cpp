#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
  ite_row = new Row(rid);
  ite_tableHeap = table_heap;
  ite_txn = txn;
}

TableIterator::TableIterator(const TableIterator &other) {
  this->ite_row = new Row(*(other.ite_row));
  this->ite_tableHeap = other.ite_tableHeap;
}

TableIterator::~TableIterator() {
  if(ite_row != nullptr){
    delete ite_row;
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if(ite_row->GetRowId().GetPageId() == itr.ite_row->GetRowId().GetPageId()){
    return true;
  }else{
    return false;
  }
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  if((*this) == itr){
    return false;
  }else{
    return true;
  }
}

const Row &TableIterator::operator*() {
  //ASSERT(false, "Not implemented yet.");
  return *ite_row;
}

Row *TableIterator::operator->() {
  //return nullptr;
  return ite_row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  //ASSERT(false, "Not implemented yet.");
  ite_tableHeap = itr.ite_tableHeap;
  (*ite_row) = (*itr.ite_row);
  return (*this);
}

// ++iter
TableIterator &TableIterator::operator++() {
  if(ite_tableHeap == nullptr){
    LOG(ERROR)<<"Unkown mistake!"<<std::endl;
  }
  auto page = reinterpret_cast<TablePage*>(ite_tableHeap->buffer_pool_manager_->FetchPage(ite_row->GetRowId().GetPageId()));
  if(page == nullptr){
    ite_row->SetRowId(RowId(INVALID_PAGE_ID, 0));
    return *this;
    LOG(ERROR)<<"Unkown mistake!"<<std::endl;
  }
  RowId next_row_id;
  if(page->GetNextTupleRid(ite_row->GetRowId(), &next_row_id)){
    ite_row->SetRowId(next_row_id);
    ite_tableHeap->GetTuple(ite_row, nullptr);
    ite_tableHeap->buffer_pool_manager_->UnpinPage(page->GetNextPageId(), false);
    return *this;
  }
  ite_tableHeap->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  while(page->GetNextPageId() != INVALID_PAGE_ID){
    page = reinterpret_cast<TablePage*>(ite_tableHeap->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
    if(page->GetFirstTupleRid(&next_row_id)){
      ite_row->SetRowId(next_row_id);
      ite_tableHeap->GetTuple(ite_row, nullptr);
      ite_tableHeap->buffer_pool_manager_->UnpinPage(page->GetPageId(),false);
      return *this;
    }
  }
  ite_row->SetRowId(RowId(INVALID_PAGE_ID, 0));
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  ++(*this);
  return TableIterator(*this);
  //return TableIterator(nullptr, RowId(), nullptr); 
}

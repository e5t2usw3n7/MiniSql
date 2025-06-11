#include "storage/table_heap.h"

bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
    page_id_t page_id = first_page_id_;
    TablePage *page = nullptr;

    while (true) {
        page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if (page == nullptr) return false;
        
        page->WLatch();
        if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, true);
            return true;
        }

        page_id_t next_page_id = page->GetNextPageId();
        if (next_page_id == INVALID_PAGE_ID) {
            page_id_t new_page_id;
            TablePage *new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
            if (new_page == nullptr) {
                page->WUnlatch();
                buffer_pool_manager_->UnpinPage(page_id, false);
                return false;
            }

            new_page->WLatch();
            new_page->Init(new_page_id, page_id, log_manager_, txn);
            page->SetNextPageId(new_page_id);

            page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, true);

            page_id = new_page_id;
            new_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(page_id, true);
            continue;
        }

        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(page_id, false);
        page_id = next_page_id;
    }
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return false;
    }

    page->WLatch();
    page->MarkDelete(rid, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    return true;
}

bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) {
        return false;
    }

    Row old_row(rid);
    page->WLatch();
    if (!page->GetTuple(&old_row, schema_, txn, lock_manager_)) {
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
        return false;
    }

    bool updated = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), updated);
    return updated;
}

void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    if (page == nullptr) return;

    page->WLatch();
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    assert(page != nullptr);
    page->WLatch();
    page->RollbackDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

bool TableHeap::GetTuple(Row *row, Txn *txn) { 
    TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
    if (page == nullptr) return false;
    
    page->RLatch();
    bool found = page->GetTuple(row, schema_, txn, lock_manager_);
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
    return found;
}

void TableHeap::DeleteTable(page_id_t page_id) {
    if (page_id != INVALID_PAGE_ID) {
        auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
        if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) {
            DeleteTable(temp_table_page->GetNextPageId());
        }
        buffer_pool_manager_->UnpinPage(page_id, false);
        buffer_pool_manager_->DeletePage(page_id);
    } else {
        DeleteTable(first_page_id_);
    }
}

TableIterator TableHeap::Begin(Txn *txn) { 
    RowId first_rid;
    TablePage *first_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
    
    if (first_page != nullptr) {
        first_page->RLatch();
        bool found = first_page->GetFirstTupleRid(&first_rid);
        first_page->RUnlatch();
        buffer_pool_manager_->UnpinPage(first_page_id_, false);
        
        if (!found) {
            first_rid = RowId(INVALID_PAGE_ID, -1);
        }
    }
    
    return TableIterator(this, first_rid, txn);
}

TableIterator TableHeap::End() {  
  return TableIterator(this, RowId(INVALID_PAGE_ID, -1), nullptr);
}

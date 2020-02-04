//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  assert(page_id != INVALID_PAGE_ID);
  frame_id_t frame_id;
  Page *page, *victim;

  auto search = page_table_.find(page_id);
  if (search != page_table_.end()) {  // present in the buffer pool
    frame_id = search->second;
    page = &pages_[frame_id];
    ++page->pin_count_;
    return page;
  } else {                      // page table miss, find a frame id
    if (!free_list_.empty()) {  // find from the free list
      frame_id = free_list_.front();
      free_list_.pop_front();
    } else if (replacer_->Victim(&frame_id)) {  // find from the replacer
      victim = &pages_[frame_id];
      if (victim->IsDirty()) {
        disk_manager_->WritePage(victim->GetPageId(), victim->GetData());
      }
      page_table_.erase(victim->GetPageId());
    } else {  // no available frame id
      return nullptr;
    }
  }

  page_table_.insert({page_id, frame_id});
  // update metadata
  page = &pages_[frame_id];
  page->page_id_ = page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  disk_manager_->ReadPage(page_id, page->data_);

  return page;
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  assert(page_id != INVALID_PAGE_ID);
  auto search = page_table_.find(page_id);
  if (search != page_table_.end()) {
    frame_id_t frame_id = search->second;
    Page *page = &pages_[frame_id];
    if (page->pin_count_ <= 0) {
      return false;
    }
    if (--page->pin_count_ == 0) {
      replacer_->Unpin(frame_id);
    }
    if (is_dirty) {
      page->is_dirty_ = true;
    }
    return true;
  }
  return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  assert(page_id != INVALID_PAGE_ID);
  auto search = page_table_.find(page_id);
  if (search != page_table_.end()) {
    frame_id_t frame_id = search->second;
    Page *page = &pages_[frame_id];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page_id, page->GetData());
    }
    return true;
  }
  return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  frame_id_t frame_id;
  Page *page, *victim;
  if (!free_list_.empty()) {  // find from the free list
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (replacer_->Victim(&frame_id)) {  // find from the replacer
    victim = &pages_[frame_id];
    if (victim->IsDirty()) {
      disk_manager_->WritePage(victim->GetPageId(), victim->GetData());
    }
    page_table_.erase(victim->GetPageId());
  } else {  // no available frame id
    return nullptr;
  }

  *page_id = disk_manager_->AllocatePage();
  page_table_.insert({*page_id, frame_id});
  // update metadata
  page = &pages_[frame_id];
  page->page_id_ = *page_id;
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->ResetMemory();

  return page;
}  // namespace bustub

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  // 0.   Make sure you call DiskManager::DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  assert(page_id != INVALID_PAGE_ID);
  frame_id_t frame_id;
  Page *page;

  auto search = page_table_.find(page_id);
  if (search != page_table_.end()) {  // present in the buffer pool
    frame_id = search->second;
    page = &pages_[frame_id];
    if (page->pin_count_) {
      return false;
    }
    page_table_.erase(page_id);
    page->ResetMemory();
    free_list_.push_front(frame_id);
    return true;
  } else {  // page table miss
    return false;
  }
}

void BufferPoolManager::FlushAllPagesImpl() {
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = &pages_[i];
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
  }
}

}  // namespace bustub
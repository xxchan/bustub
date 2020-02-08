//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// linear_probe_hash_table.cpp
//
// Identification: src/container/hash/linear_probe_hash_table.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "container/hash/linear_probe_hash_table.h"

#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::LinearProbeHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                      const KeyComparator &comparator, size_t num_buckets,
                                      HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  auto header_page =
      reinterpret_cast<HashTableHeaderPage *>(buffer_pool_manager_->NewPage(&header_page_id_, nullptr)->GetData());
  header_page->SetPageId(header_page_id_);
  header_page->SetSize(num_buckets);
  for (size_t i = 0; i < GetBlockNum(); ++i) {
    page_id_t block_page_id;
    buffer_pool_manager_->NewPage(&block_page_id, nullptr);
    header_page->AddBlockPageId(block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, false, nullptr);
  }
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  slot_offset_t slot_offset_begin = hash_fn_.GetHash(key) % GetSize();
  size_t block_index = slot_offset_begin / BLOCK_ARRAY_SIZE;
  slot_offset_t block_offset = slot_offset_begin % BLOCK_ARRAY_SIZE;
  auto block_page = GetBlockPage(block_index);
  while (block_page->IsOccupied(block_offset)) {
    if (block_page->IsReadable(block_offset)) {
      auto curr_key = block_page->KeyAt(block_offset);
      if (comparator_(curr_key, key) == 0) {
        result->push_back(block_page->ValueAt(block_offset));
      }
    }
    block_offset++;
    if (block_offset == BLOCK_ARRAY_SIZE) {
      block_offset = 0;
      block_index = (block_index + 1) % GetBlockNum();
      block_page = GetBlockPage(block_index);
    }
    if (block_index * BLOCK_ARRAY_SIZE + block_offset == slot_offset_begin) {
      break;
    }
  }
  return !result->empty();
}
/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  slot_offset_t slot_offset_begin = hash_fn_.GetHash(key) % GetSize();
  size_t block_index = slot_offset_begin / BLOCK_ARRAY_SIZE;
  slot_offset_t block_offset = slot_offset_begin % BLOCK_ARRAY_SIZE;
  auto block_page = GetBlockPage(block_index);
  bool ret = false;
  while (!(ret = block_page->Insert(block_offset, key, value))) {
    if (block_page->IsReadable(block_offset)) {
      auto curr_key = block_page->KeyAt(block_offset);
      auto curr_value = block_page->ValueAt(block_offset);
      if (comparator_(curr_key, key) == 0 && curr_value == value) {  // K-V pair exists (Is '==' for value OK?)
        break;
      }
    }
    block_offset++;
    if (block_offset == BLOCK_ARRAY_SIZE) {
      block_offset = 0;
      block_index = (block_index + 1) % GetBlockNum();
      block_page = GetBlockPage(block_index);
    }
    if (block_index * BLOCK_ARRAY_SIZE + block_offset == slot_offset_begin) {  // hash table is full
      Resize(GetSize());
      return Insert(transaction, key, value);
    }
  }

  return ret;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  slot_offset_t slot_offset_begin = hash_fn_.GetHash(key) % GetSize();
  size_t block_index = slot_offset_begin / BLOCK_ARRAY_SIZE;
  slot_offset_t block_offset = slot_offset_begin % BLOCK_ARRAY_SIZE;
  auto block_page = GetBlockPage(block_index);
  while (block_page->IsOccupied(block_offset)) {
    if (block_page->IsReadable(block_offset)) {
      auto curr_key = block_page->KeyAt(block_offset);
      auto curr_value = block_page->ValueAt(block_offset);
      if (comparator_(curr_key, key) == 0 && curr_value == value) {  // Is '==' for value OK?
        block_page->Remove(block_offset);
        return true;
      }
    }
    block_offset++;
    if (block_offset == BLOCK_ARRAY_SIZE) {
      block_offset = 0;
      block_index = (block_index + 1) % GetBlockNum();
      block_page = GetBlockPage(block_index);
    }
    if (block_index * BLOCK_ARRAY_SIZE + block_offset == slot_offset_begin) {
      break;
    }
  }
  return false;
}

/*****************************************************************************
 * RESIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  auto header_page = GetHeaderPage();
  auto old_block_num = GetBlockNum();
  header_page->SetSize(2 * initial_size);
  for (size_t i = old_block_num; i < GetBlockNum(); ++i) {
    page_id_t block_page_id;
    buffer_pool_manager_->NewPage(&block_page_id, nullptr);
    header_page->AddBlockPageId(block_page_id);
    buffer_pool_manager_->UnpinPage(block_page_id, false, nullptr);
  }

  size_t block_index = 0;
  slot_offset_t block_offset = 0;
  auto block_page = GetBlockPage(block_index);
  while (block_page->IsOccupied(block_offset) && block_index < old_block_num) {
    if (block_page->IsReadable(block_offset)) {
      auto curr_key = block_page->KeyAt(block_offset);
      auto curr_slot_offset = hash_fn_.GetHash(curr_key) % GetSize();
      auto curr_block_index = curr_slot_offset / GetBlockNum();
      if (curr_block_index >= old_block_num) {
        // can be moved to new allocated block
        // leave tombstone in the original slot
        auto curr_value = block_page->ValueAt(block_offset);
        // can be improved, no need to search
        Remove(nullptr, curr_key, curr_value);
        Insert(nullptr, curr_key, curr_value);
      }
    }
    block_offset++;
    if (block_offset == BLOCK_ARRAY_SIZE) {
      block_offset = 0;
      ++block_index;
      block_page = GetBlockPage(block_index);
    }
  }
}

/*****************************************************************************
 * GETSIZE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return GetHeaderPage()->GetSize();
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

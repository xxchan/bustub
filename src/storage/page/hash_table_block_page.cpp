//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_block_page.cpp
//
// Identification: src/storage/page/hash_table_block_page.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_block_page.h"

#include "storage/index/generic_key.h"
namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BLOCK_TYPE::KeyAt(slot_offset_t bucket_ind) const {
  assert(bucket_ind < BLOCK_ARRAY_SIZE);
  return array_[bucket_ind].first;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BLOCK_TYPE::ValueAt(slot_offset_t bucket_ind) const {
  assert(bucket_ind < BLOCK_ARRAY_SIZE);
  return array_[bucket_ind].second;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::Insert(slot_offset_t bucket_ind, const KeyType &key, const ValueType &value) {
  assert(bucket_ind < BLOCK_ARRAY_SIZE);
  if (IsOccupied(bucket_ind)) {
    return false;
  }
  array_[bucket_ind] = {key, value};
  SetOccupied(bucket_ind, true);
  SetReadable(bucket_ind, true);
  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::Remove(slot_offset_t bucket_ind) {
  assert(bucket_ind < BLOCK_ARRAY_SIZE);
  SetReadable(bucket_ind, false);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsOccupied(slot_offset_t bucket_ind) const {
  assert(bucket_ind < BLOCK_ARRAY_SIZE);
  unsigned char mask = (1U << (bucket_ind % 8));
  unsigned char byte = occupied_[bucket_ind / 8];
  return (mask & byte) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BLOCK_TYPE::IsReadable(slot_offset_t bucket_ind) const {
  assert(bucket_ind < BLOCK_ARRAY_SIZE);
  unsigned char mask = (1U << (bucket_ind % 8));
  unsigned char byte = readable_[bucket_ind / 8];
  return (mask & byte) != 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetOccupied(slot_offset_t bucket_ind, bool flag) {
  assert(bucket_ind < BLOCK_ARRAY_SIZE);
  unsigned char mask = (1U << (bucket_ind % 8));
  if (flag) {
    occupied_[bucket_ind / 8] |= mask;
  } else {
    occupied_[bucket_ind / 8] &= ~mask;
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BLOCK_TYPE::SetReadable(slot_offset_t bucket_ind, bool flag) {
  assert(bucket_ind < BLOCK_ARRAY_SIZE);
  unsigned char mask = (1U << (bucket_ind % 8));
  if (flag) {
    readable_[bucket_ind / 8] |= mask;
  } else {
    readable_[bucket_ind / 8] &= ~mask;
  }
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBlockPage<int, int, IntComparator>;
template class HashTableBlockPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBlockPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBlockPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBlockPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBlockPage<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

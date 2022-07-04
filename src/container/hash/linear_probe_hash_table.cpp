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
    : buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      num_buckets(num_buckets),
      hash_fn_(std::move(hash_fn)) {
  auto page = buffer_pool_manager->NewPage(&header_page_id_);
  page->WLatch();

  auto header_page = HeaderPageCast(page);
  InitHeaderPage(header_page, num_buckets);

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  // get the header page
  auto raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  raw_header_page->RLatch();
  auto header_page = HeaderPageCast(raw_header_page);

  // get slot index, block page index and bucket index according to key
  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  // get block page that contains the key
  auto raw_block_page = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_index));
  raw_block_page->RLatch();
  auto block_page = BlockPageCast(raw_block_page);

  // linear probe
  while (block_page->IsOccupied(bucket_index)) {
    // find the correct position
    if (block_page->IsReadable(bucket_index) && !comparator_(key, block_page->KeyAt(bucket_index))) {
      result->push_back(block_page->ValueAt(bucket_index));
    }

    // step forward
    StepForward(&bucket_index, &block_index, header_page, raw_block_page, block_page, LockType::READ);

    // break loop if we have returned to original position
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      break;
    }
  }

  // unlock
  raw_block_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), false);
  raw_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return result->size() > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // get the header page
  auto raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  raw_header_page->RLatch();
  auto header_page = HeaderPageCast(raw_header_page);

  // get slot index, block page index and bucket index according to key
  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  // get block page that contains the key
  auto raw_block_page = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_index));
  raw_block_page->WLatch();
  auto block_page = BlockPageCast(raw_block_page);

  bool success = true;
  while (!block_page->Insert(bucket_index, key, value)) {
    // return false if (key, value) pair already exists
    if (IsMatch(block_page, bucket_index, key, value)) {
      success = false;
      break;
    }

    // step forward
    StepForward(&bucket_index, &block_index, header_page, raw_block_page, block_page, LockType::WRITE);

    // resize hash table if we have returned to original position
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      raw_block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), success);
      raw_header_page->RUnlatch();
      buffer_pool_manager_->UnpinPage(header_page_id_, false);

      // resize
      Resize(header_page->NumBlocks() * BLOCK_ARRAY_SIZE);

      // recalculate index
      raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_);
      raw_header_page->WLatch();
      header_page = HeaderPageCast(raw_header_page);
      std::tie(slot_index, block_index, bucket_index) = GetIndex(key);

      raw_block_page = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_index));
      raw_block_page->WLatch();
      block_page = BlockPageCast(raw_block_page);
    }
  }

  raw_block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), success);
  raw_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // get the header page
  auto raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  raw_header_page->RLatch();
  auto header_page = HeaderPageCast(raw_header_page);

  // get slot index, block page index and bucket index according to key
  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  // get block page that contains the key
  auto raw_block_page = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(block_index));
  raw_block_page->WLatch();
  auto block_page = BlockPageCast(raw_block_page);

  bool success = false;
  while (block_page->IsOccupied(bucket_index)) {
    // remove the (key, value) pair if find the matched readable one
    if (IsMatch(block_page, bucket_index, key, value)) {
      if (block_page->IsReadable(bucket_index)) {
        block_page->Remove(bucket_index);
        success = true;
      } else {
        success = false;
      }
      break;
    }

    // step forward
    StepForward(&bucket_index, &block_index, header_page, raw_block_page, block_page, LockType::WRITE);

    // break loop if we have returned to original position
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      break;
    }
  }

  raw_block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), success);
  raw_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();

  // get the old header page
  auto raw_old_header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  raw_old_header_page->RLatch();
  auto old_header_page = HeaderPageCast(raw_old_header_page);

  // get the new header page
  auto raw_header_page = buffer_pool_manager_->NewPage(&header_page_id_);
  raw_header_page->WLatch();
  auto header_page = HeaderPageCast(raw_header_page);
  InitHeaderPage(header_page, 2 * initial_size / BLOCK_ARRAY_SIZE);

  // move (key, value) pairs to new space
  for (size_t block_index = 0; block_index < old_header_page->NumBlocks(); ++block_index) {
    auto raw_block_page = buffer_pool_manager_->FetchPage(old_header_page->GetBlockPageId(block_index));
    raw_block_page->RLatch();
    HASH_TABLE_BLOCK_TYPE *block_page = BlockPageCast(raw_block_page);

    // move (key, value) pair from each readable slot
    for (slot_offset_t bucket_index = 0; bucket_index < BLOCK_ARRAY_SIZE; ++bucket_index) {
      if (block_page->IsReadable(bucket_index)) {
        Insert(nullptr, block_page->KeyAt(bucket_index), block_page->ValueAt(bucket_index));
      }
    }

    // delete old page
    raw_block_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(raw_block_page->GetPageId());
  }

  raw_header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
  raw_old_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(raw_old_header_page->GetPageId(), false);
  buffer_pool_manager_->DeletePage(raw_old_header_page->GetPageId());
  table_latch_.WUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  table_latch_.RLock();

  // get the header page
  auto raw_header_page = buffer_pool_manager_->FetchPage(header_page_id_);
  raw_header_page->RLatch();
  HashTableHeaderPage *header_page = HeaderPageCast(raw_header_page);

  // get the number of block page
  size_t num_buckets = header_page->NumBlocks();

  raw_header_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  table_latch_.RUnlock();
  return num_buckets * BLOCK_ARRAY_SIZE;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::InitHeaderPage(HashTableHeaderPage *page, size_t num_buckets) {
  page->SetPageId(header_page_id_);
  page->SetSize(num_buckets);

  page_id_t page_id;
  for (size_t i = 0; i < num_buckets; ++i) {
    buffer_pool_manager_->NewPage(&page_id);
    page->AddBlockPageId(page_id);
    buffer_pool_manager_->UnpinPage(page_id, false);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
std::tuple<size_t, page_id_t, slot_offset_t> HASH_TABLE_TYPE::GetIndex(const KeyType &key) {
  size_t slot_index = hash_fn_.GetHash(key) % (BLOCK_ARRAY_SIZE * num_buckets);
  page_id_t block_index = slot_index / BLOCK_ARRAY_SIZE;
  slot_offset_t bucket_index = block_index % BLOCK_ARRAY_SIZE;
  return {slot_index, block_index, bucket_index};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::StepForward(slot_offset_t *bucket_index, page_id_t *block_index, HashTableHeaderPage *header_page,
                                  Page *raw_block_page, [[maybe_unused]]HASH_TABLE_BLOCK_TYPE * block_page,
                                  LockType lockType) {
  (*bucket_index)++;

  if (*bucket_index != BLOCK_ARRAY_SIZE) {
    return;
  }

  // move to next block page
  if (lockType == LockType::READ) {
    raw_block_page->RUnlatch();
  } else {
    raw_block_page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), false);

  // update index
  *bucket_index = 0;
  *block_index = (*block_index + 1) % header_page->NumBlocks();

  // update page
  raw_block_page = buffer_pool_manager_->FetchPage(header_page->GetBlockPageId(*block_index));
  if (lockType == LockType::READ) {
    raw_block_page->RLatch();
  } else {
    raw_block_page->WLatch();
  }
  block_page = BlockPageCast(raw_block_page);
}

template class LinearProbeHashTable<int, int, IntComparator>;

template class LinearProbeHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class LinearProbeHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class LinearProbeHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class LinearProbeHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class LinearProbeHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub

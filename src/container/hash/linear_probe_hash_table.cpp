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
      num_buckets_(num_buckets),
      num_pages_((num_buckets - 1) / BLOCK_ARRAY_SIZE + 1),
      last_block_array_size_(num_buckets - (num_pages_ - 1) * BLOCK_ARRAY_SIZE),
      hash_fn_(std::move(hash_fn)) {
  auto page = buffer_pool_manager->NewPage(&header_page_id_);
  page->WLatch();

  InitHeaderPage(HeaderPageCast(page));

  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, true);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();

  // get slot index, block page index and bucket index according to key
  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  // get block page that contains the key
  auto raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  raw_block_page->RLatch();
  auto block_page = BlockPageCast(raw_block_page);

  // linear probe
  while (block_page->IsOccupied(bucket_index)) {
    // find the correct position
    if (block_page->IsReadable(bucket_index) && !comparator_(key, block_page->KeyAt(bucket_index))) {
      result->push_back(block_page->ValueAt(bucket_index));
    }

    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::READ);

    // break loop if we have returned to original position
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      break;
    }
  }

  // unlock
  raw_block_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), false);
  table_latch_.RUnlock();
  return result->size() > 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  auto success = InsertImpl(transaction, key, value);
  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::InsertImpl(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // get slot index, block page index and bucket index according to key
  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  // get block page that contains the key
  auto raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
  raw_block_page->WLatch();
  auto block_page = BlockPageCast(raw_block_page);

  bool success = true;
  while (!block_page->Insert(bucket_index, key, value)) {
    // return false if (key, value) pair already exists
    if (block_page->IsReadable(bucket_index) && IsMatch(block_page, bucket_index, key, value)) {
      success = false;
      break;
    }

    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::WRITE);

    // resize hash table if we have returned to original position
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      raw_block_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), false);

      Resize(num_pages_);
      std::tie(slot_index, block_index, bucket_index) = GetIndex(key);

      raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
      raw_block_page->WLatch();
      block_page = BlockPageCast(raw_block_page);
    }
  }

  raw_block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), success);
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();

  // get slot index, block page index and bucket index according to key
  auto [slot_index, block_index, bucket_index] = GetIndex(key);

  // get block page that contains the key
  auto raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
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
    StepForward(bucket_index, block_index, raw_block_page, block_page, LockType::WRITE);

    // break loop if we have returned to original position
    if (block_index * BLOCK_ARRAY_SIZE + bucket_index == slot_index) {
      break;
    }
  }

  raw_block_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(raw_block_page->GetPageId(), success);
  table_latch_.RUnlock();
  return success;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Resize(size_t initial_size) {
  table_latch_.WLock();
  num_buckets_ = 2 * initial_size;
  num_pages_ = (num_buckets_ - 1) / BLOCK_ARRAY_SIZE + 1;
  last_block_array_size_ = num_buckets_ - (num_pages_ - 1) * BLOCK_ARRAY_SIZE;

  // save the old header page id
  auto old_header_page_id = header_page_id_;
  std::vector<page_id_t> old_page_ids(page_ids_);

  // get the new header page
  auto raw_header_page = buffer_pool_manager_->NewPage(&header_page_id_);
  raw_header_page->WLatch();
  InitHeaderPage(HeaderPageCast(raw_header_page));

  // move (key, value) pairs to new space
  for (size_t block_index = 0; block_index < num_pages_; ++block_index) {
    auto old_page_id = old_page_ids[block_index];
    auto raw_block_page = buffer_pool_manager_->FetchPage(old_page_id);
    raw_block_page->RLatch();
    auto block_page = BlockPageCast(raw_block_page);

    // move (key, value) pair from each readable slot
    for (slot_offset_t bucket_index = 0; bucket_index < GetBlockArraySize(block_index); ++bucket_index) {
      if (block_page->IsReadable(bucket_index)) {
        InsertImpl(nullptr, block_page->KeyAt(bucket_index), block_page->ValueAt(bucket_index));
      }
    }

    // delete old page
    raw_block_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(old_page_id, false);
    buffer_pool_manager_->DeletePage(old_page_id);
  }

  raw_header_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(header_page_id_, false);
  buffer_pool_manager_->DeletePage(old_header_page_id);
  table_latch_.WUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
size_t HASH_TABLE_TYPE::GetSize() {
  return num_buckets_;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::InitHeaderPage(HashTableHeaderPage *header_page) {
  header_page->SetPageId(header_page_id_);
  header_page->SetSize(num_buckets_);

  page_ids_.clear();
  for (size_t i = 0; i < num_pages_; ++i) {
    page_id_t page_id;
    buffer_pool_manager_->NewPage(&page_id);
    buffer_pool_manager_->UnpinPage(page_id, false);
    header_page->AddBlockPageId(page_id);
    page_ids_.push_back(page_id);
  }
}

template <typename KeyType, typename ValueType, typename KeyComparator>
auto HASH_TABLE_TYPE::GetIndex(const KeyType &key) -> std::tuple<slot_index_t, block_index_t, slot_offset_t> {
  slot_index_t slot_index = hash_fn_.GetHash(key) % num_buckets_;
  block_index_t block_index = slot_index / BLOCK_ARRAY_SIZE;
  slot_offset_t bucket_index = slot_index % BLOCK_ARRAY_SIZE;
  return {slot_index, block_index, bucket_index};
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::StepForward(slot_offset_t &bucket_index, block_index_t &block_index, Page *&raw_block_page,
                                  HASH_TABLE_BLOCK_TYPE *&block_page, LockType lockType) {
  if (++bucket_index != GetBlockArraySize(block_index)) {
    return;
  }

  // move to next block page
  if (lockType == LockType::READ) {
    raw_block_page->RUnlatch();
  } else {
    raw_block_page->WUnlatch();
  }
  buffer_pool_manager_->UnpinPage(page_ids_[block_index], false);

  // update index
  bucket_index = 0;
  block_index = (block_index + 1) % num_pages_;

  // update page
  raw_block_page = buffer_pool_manager_->FetchPage(page_ids_[block_index]);
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

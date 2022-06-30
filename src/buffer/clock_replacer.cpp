//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

#include <cassert>

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : clock_hand_(-1) {
  for (size_t i = 0; i < num_pages; ++i) {
    frames_.push_back(std::make_tuple(false, false));
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  assert(static_cast<size_t>(*frame_id) < frames_.size());
  if (Size() == 0) {
    return false;
  }

  std::lock_guard<std::shared_mutex> lock(mutex_);
  while (true) {
    clock_hand_ = (clock_hand_ + 1) % frames_.size();
    auto &[contains, ref] = frames_[clock_hand_];
    if (contains) {
      if (ref) {
        ref = false;
      } else {
        *frame_id = clock_hand_;
        contains = false;
        return true;
      }
    }
  }
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  assert(static_cast<size_t>(frame_id) < frames_.size());
  std::lock_guard<std::shared_mutex> lock(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = false;
  ref = false;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  assert(static_cast<size_t>(frame_id) < frames_.size());
  std::lock_guard<std::shared_mutex> lock(mutex_);
  auto &[contains, ref] = frames_[frame_id];
  contains = true;
  ref = true;
}

size_t ClockReplacer::Size() {
  std::shared_lock<std::shared_mutex> lock(mutex_);
  size_t size = 0;
  for (auto &[contains, ref] : frames_) {
    size += contains;
  }
  return size;
}
}  // namespace bustub

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

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
  num_frames = num_pages;
  pin = new bool[num_frames];
  ref = new bool[num_frames];
  for (size_t i = 0; i < num_frames; ++i) {
    pin[i] = true;
    ref[i] = false;
  }
  hand = 0;
}

ClockReplacer::~ClockReplacer() {
  delete[] pin;
  delete[] ref;
}

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  size_t i = hand, first_unpinned = static_cast<size_t>(-1);
  do {
    if (!pin[i]) {
      first_unpinned = i;
      break;
    }
    ref[i] = false;
    i = (i + 1) % num_frames;
  } while (i != hand);

  if (first_unpinned == static_cast<size_t>(-1)) {  // all frames pinned
    return false;
  }

  do {
    if (!pin[i] && !ref[i]) {
      *frame_id = i;
      hand = i;
      pin[i] = true;
      return true;
    }
    ref[i] = false;
    i = (i + 1) % num_frames;
  } while (i != hand);

  *frame_id = first_unpinned;
  hand = first_unpinned;
  pin[hand] = true;
  return true;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  //   assert(0 <= frame_id && frame_id < num_frames);
  pin[frame_id] = true;
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  //   assert(0 <= frame_id && frame_id < num_frames);
  pin[frame_id] = false;
  ref[frame_id] = true;
}

size_t ClockReplacer::Size() {
  size_t cnt = 0;
  for (size_t i = 0; i < num_frames; ++i) {
    cnt += pin[i] ? 0 : 1;
  }
  return cnt;
}

}  // namespace bustub

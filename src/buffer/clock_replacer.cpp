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
#include "assert.h"

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
  size_t i = hand;
  do {
    if (i == num_frames) {
      i = 0;
    }
    if (!pin[i] && !ref[i]) {
      *frame_id = i;
      hand = i;
      pin[i] = true;
      return true;
    }
    ref[i] = false;
    ++i;
  } while (i != hand);

  if (!pin[hand]) {
    *frame_id = hand;
    pin[hand] = true;
    return true;
  } else {
    return false;
  }
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
    cnt += !pin[i];
  }
  return cnt;
}

}  // namespace bustub

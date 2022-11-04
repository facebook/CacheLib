/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once
#include <algorithm>
#include <cstdint>

#include "cachelib/common/CompilerUtils.h"

namespace facebook::cachelib::cachebench {
/**
 * CacheValue is the schema for all items in cachebench.
 * It reserves first few bytes as count/flag for special
 * purpose of sanity check (consistency check, destructor check).
 *
 * Additional fields can be added for future sanity check, but if
 * the field is complex and contains pointers please use ObjectCache.
 * The CacheValue object is treated as blobs and stored in cachelib allocator.
 *
 * Usage:
 * auto handle = cache_->allocate(pid, key, CacheValue::getSize(size));
 * if (handle) {
 *   CacheValue::initialize(handle->getMemory());
 * }
 *
 * auto* value = item->template getMemoryAs<CacheValue>();
 * value->setVersion(...);
 * std::memcpy(value->getData(), ...);
 */
class CACHELIB_PACKED_ATTR CacheValue {
 public:
  // returns an unique id, used for destructor check
  uint64_t getIdx() const { return idx_; }
  // returns a version number, used for destructor check
  uint64_t getVersion() const { return version_; }
  // returns a number used for consistency check
  uint64_t getConsistencyNum() const { return consistencyNum_; }
  // returns writable data pointer
  void* getData() { return data_; }
  // returns the data pointer
  const void* getData() const { return data_; }
  // returns the data size for given item size, this substrcut
  // the size of all fields used for sanity checks.
  uint32_t getDataSize(uint32_t size) const {
    return size > sizeof(CacheValue) ? size - sizeof(CacheValue) : 0;
  }

  // set the unique id, used for destructor check
  void setIdx(uint64_t idx) { idx_ = idx; }
  // set the version number, used for destructor check
  void setVersion(uint64_t version) { version_ = version; }
  // increment the version number by one, used for destructor check
  void incrVersion() { ++version_; }
  // set the number for consistency check
  void setConsistencyNum(uint64_t num) { consistencyNum_ = num; }

  // static function to make sure the item size is at least sizeof CacheValue
  // so we have enough space for fields used by sanity checks
  static size_t getSize(size_t size) {
    return std::max<size_t>(size, sizeof(CacheValue));
  }
  // static function to help to initialize CacheValue with given item's data
  // pointer.
  static void initialize(void* ptr) { new (ptr) CacheValue(); }

 private:
  // for ItemDestructorCheck usage
  uint64_t idx_;        // an unique id for item in cache
  uint64_t version_{0}; // a version number for item due to in-place mutation
  // for ConsistencyCheck usage
  uint64_t consistencyNum_{0}; // a number to check for item validation
  // beginning of the byte array.
  unsigned char data_[0];
};

} // namespace facebook::cachelib::cachebench

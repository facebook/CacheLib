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

#include <folly/Portability.h>
#include <folly/Range.h>
#include <folly/io/IOBuf.h>

#include "cachelib/allocator/memory/Slab.h"
namespace facebook {
namespace cachelib {

// encapsulates an item's payload and its original allocation size. We need to
// preserve the original allocation size and at the same time copy all the
// bytes of an item's allocation.
struct Blob {
  // original allocation size requested by the cachelib user through
  // allocate() call.
  uint32_t origAllocSize{0};

  // full payload including the trailing bytes
  folly::StringPiece data;
};

// NvmItem is used to store CacheItems in nvm cache.
class FOLLY_PACK_ATTR NvmItem {
 public:
  // constructs a nvm item with multiple blob
  //
  // @param id            pool id for the original item
  // @param creationTime  creation time for the item in cache
  // @param blobs         vector of blobs
  //
  // @throw std::out_of_range if the total size of the blobs exceeds 4GB.
  NvmItem(PoolId id,
          uint32_t creationTime,
          uint32_t expTime,
          const std::vector<Blob>& blobs);

  //  same as the above, but handles for a single blob without having to
  //  instantiate a vector
  //
  // @throw std::out_of_range if the total size of blob exceeds 4GB.
  NvmItem(PoolId id, uint32_t creationTime, uint32_t expTime, Blob blob);

  // A custom new that allocates NvmItem with extra
  // bytes space at the end for data
  static void* operator new(size_t count, size_t extra);

  // Because we alloc extra (unknowable) amount of space, we cannot use sized
  // deallocation. So override `delete`, without sized deallocation, so this
  // will always get chosen regardless of global overrides.
  static void operator delete(void* p);

  // This delete operator overload is specifically here to complement the
  // custom placement new operator in the case of an exception from constructor.
  // C++14 doc also requires this delete operator must be overloaded with the
  // one above together.
  static void operator delete(void* p, size_t);

  // @return    pool id where the original cache item was
  //            stored
  PoolId poolId() const noexcept { return id_; }

  // @return the time when the object was originally created in Cache.
  uint32_t getCreationTime() const noexcept { return creationTime_; }

  // @return the time when the item is going to expire in seconds since epoch
  // format.
  uint32_t getExpiryTime() const noexcept { return expTime_; }

  // number of blobs in this nvm item
  size_t getNumBlobs() const noexcept { return numBlobs_; }

  // get the blob at index. index starts from 0 up to numBlobs - 1
  //
  // @throw std::invalid_argument if the index is out of range.
  Blob getBlob(size_t index) const;

  // return true if the item is expired
  bool isExpired() const noexcept;

  // @return    total size of this item including data for all the blobs. This
  // should be alteast  estimateVariableSize() + sizeof(NvmItem)
  size_t totalSize() const noexcept;

  // estimate the additional malloc size for a single blob to be passed to the
  // new operator
  static size_t estimateVariableSize(Blob blob);

  // estimate the additional  malloc size for a vector of blobs
  static size_t estimateVariableSize(const std::vector<Blob>& blobs);

 private:
  // returns the pointer to the beginning of the blob array.
  const char* getDataCBegin() const {
    return reinterpret_cast<const char*>(data_ + numBlobs_ * sizeof(BlobInfo));
  }

  char* getDataBegin() { return const_cast<char*>(getDataCBegin()); }

  // for each blob, we need to store its original size intended and the actual
  // size by storing its end offset
  struct FOLLY_PACK_ATTR BlobInfo {
    uint32_t origAllocSize;
    uint32_t endOffset;
  };

  // returns the blob info for the index
  BlobInfo& getBlobInfo(size_t index) {
    return *reinterpret_cast<BlobInfo*>(data_ + index * sizeof(BlobInfo));
  }

  const BlobInfo& getBlobInfo(size_t index) const {
    return *reinterpret_cast<const BlobInfo*>(data_ + index * sizeof(BlobInfo));
  }

  /* --Layout--
   * Member fields
   *
   * BlobInfo[0]
   * .
   * .
   * .
   * BlobInfo[numBlobs_ - 1]
   * Blobs[0]
   * .
   * .
   * .
   * Blobs[numBlobs_ - 1]
   */

  const PoolId id_; // pool id of the cache item
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-private-field"
#endif
  const uint8_t flags_ = 0; // flags for the item. Currently unused.
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
  const uint32_t creationTime_; // creation time in seconds since epoch
  const uint32_t expTime_;      // seconds since epoch when the item expires
  const size_t numBlobs_;       // total number of blobs
  uint8_t data_[];              // variable sized payload
};

namespace detail {
inline void NvmItemFreeCb(void* buf, void* /* userData */) {
  delete reinterpret_cast<NvmItem*>(buf);
}
} // namespace detail

inline folly::IOBuf toIOBuf(std::unique_ptr<NvmItem> nitem) {
  const auto size = nitem->totalSize();
  return folly::IOBuf{folly::IOBuf::TAKE_OWNERSHIP, nitem.release(), size,
                      detail::NvmItemFreeCb};
}
} // namespace cachelib
} // namespace facebook

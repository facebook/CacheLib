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

#include "cachelib/allocator/nvmcache/NvmItem.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop

#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {

Blob NvmItem::getBlob(size_t index) const {
  if (index >= numBlobs_) {
    throw std::invalid_argument(
        folly::sformat("Index {} out of range {}", index, numBlobs_));
  }

  const auto& blobInfo = getBlobInfo(index);
  const size_t begin = index == 0 ? 0 : getBlobInfo(index - 1).endOffset;
  const size_t size = blobInfo.endOffset - begin;

  folly::StringPiece data(getDataCBegin() + begin, size);
  return Blob{blobInfo.origAllocSize, data};
}

NvmItem::NvmItem(PoolId id,
                 uint32_t creationTime,
                 uint32_t expTime,
                 const std::vector<Blob>& blobs)
    : id_(id),
      creationTime_(creationTime),
      expTime_(expTime),
      numBlobs_(blobs.size()) {
  size_t offset = 0;
  char* dataBegin = getDataBegin();
  int index = 0;
  for (const auto& blob : blobs) {
    auto& blobInfo = getBlobInfo(index++);
    if (offset + blob.data.size() >
        std::numeric_limits<decltype(blobInfo.endOffset)>::max()) {
      throw std::out_of_range(folly::sformat(
          "new offset {} is out of range. blob size {}, num blobs{}",
          offset,
          blob.data.size(),
          blobs.size()));
    }

    std::memcpy(dataBegin + offset, blob.data.data(), blob.data.size());
    offset += blob.data.size();

    blobInfo.origAllocSize = blob.origAllocSize;
    blobInfo.endOffset = static_cast<uint32_t>(offset);
  }
}

NvmItem::NvmItem(PoolId id, uint32_t creationTime, uint32_t expTime, Blob blob)
    : id_(id), creationTime_(creationTime), expTime_(expTime), numBlobs_(1) {
  auto& blobInfo = getBlobInfo(0);
  if (blob.data.size() >
      std::numeric_limits<decltype(blobInfo.endOffset)>::max()) {
    throw std::out_of_range(
        folly::sformat("blob is too big. size {}", blob.data.size()));
  }
  std::memcpy(getDataBegin(), blob.data.data(), blob.data.size());
  blobInfo.origAllocSize = blob.origAllocSize;
  blobInfo.endOffset = static_cast<uint32_t>(blob.data.size());
}

void* NvmItem::operator new(size_t count, size_t extra) {
  void* alloc = malloc(count + extra);
  if (alloc == nullptr) {
    throw std::bad_alloc();
  }
  return alloc;
}

void NvmItem::operator delete(void* p) {
  free(p); // let free figure out the size.
}

void NvmItem::operator delete(void* p, size_t) { operator delete(p); }

size_t NvmItem::totalSize() const noexcept {
  // size of sizes + size of all blobs
  return sizeof(NvmItem) + numBlobs_ * sizeof(BlobInfo) +
         getBlobInfo(numBlobs_ - 1).endOffset;
}

size_t NvmItem::estimateVariableSize(const std::vector<Blob>& blobs) {
  size_t total = 0;
  for (const auto& blob : blobs) {
    total += estimateVariableSize(blob);
  }
  return total;
}

size_t NvmItem::estimateVariableSize(Blob blob) {
  return sizeof(BlobInfo) + blob.data.size();
}

bool NvmItem::isExpired() const noexcept {
  return expTime_ > 0 &&
         expTime_ < static_cast<uint32_t>(util::getCurrentTimeSec());
}

} // namespace cachelib

} // namespace facebook

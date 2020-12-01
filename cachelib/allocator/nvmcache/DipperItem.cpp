#include "cachelib/allocator/nvmcache/DipperItem.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop

#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {

Blob DipperItem::getBlob(size_t index) const {
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

DipperItem::DipperItem(PoolId id,
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

DipperItem::DipperItem(PoolId id,
                       uint32_t creationTime,
                       uint32_t expTime,
                       Blob blob)
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

void* DipperItem::operator new(size_t count, size_t extra, bool) {
  void* alloc = malloc(count + extra);
  if (alloc == nullptr) {
    throw std::bad_alloc();
  }
  return alloc;
}

void DipperItem::operator delete(void* p) {
  free(p); // let free figure out the size.
}

void DipperItem::operator delete(void* p, size_t, bool) { operator delete(p); }

size_t DipperItem::totalSize() const noexcept {
  // size of sizes + size of all blobs
  return sizeof(DipperItem) + numBlobs_ * sizeof(BlobInfo) +
         getBlobInfo(numBlobs_ - 1).endOffset;
}

size_t DipperItem::estimateVariableSize(const std::vector<Blob>& blobs) {
  size_t total = 0;
  for (const auto& blob : blobs) {
    total += estimateVariableSize(blob);
  }
  return total;
}

size_t DipperItem::estimateVariableSize(Blob blob) {
  return sizeof(BlobInfo) + blob.data.size();
}

bool DipperItem::isExpired() const noexcept {
  return expTime_ > 0 &&
         expTime_ < static_cast<uint32_t>(util::getCurrentTimeSec());
}

} // namespace cachelib

} // namespace facebook

#include "cachelib/navy/bighash/Bucket.h"
#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
static_assert(sizeof(Bucket) == 24,
              "Bucket overhead. If this changes, you may have to adjust the "
              "sizes used in unit tests.");

namespace {
// This maps to exactly how an entry is stored in a bucket on device.
class FOLLY_PACK_ATTR BucketEntry {
 public:
  static uint32_t computeSize(uint32_t keySize, uint32_t valueSize) {
    return sizeof(BucketEntry) + keySize + valueSize;
  }

  static BucketEntry& create(MutableBufferView storage,
                             HashedKey hk,
                             BufferView value) {
    new (storage.data()) BucketEntry{hk, value};
    return reinterpret_cast<BucketEntry&>(*storage.data());
  }

  BufferView key() const { return {keySize_, data_}; }

  bool keyEqualsTo(HashedKey hk) const {
    return hk == HashedKey::precomputed(key(), keyHash_);
  }

  uint64_t keyHash() const { return keyHash_; }

  BufferView value() const { return {valueSize_, data_ + keySize_}; }

 private:
  BucketEntry(HashedKey hk, BufferView value)
      : keySize_{static_cast<uint32_t>(hk.key().size())},
        valueSize_{static_cast<uint32_t>(value.size())},
        keyHash_{hk.keyHash()} {
    static_assert(sizeof(BucketEntry) == 16, "BucketEntry overhead");
    hk.key().copyTo(data_);
    value.copyTo(data_ + keySize_);
  }

  const uint32_t keySize_{};
  const uint32_t valueSize_{};
  const uint64_t keyHash_{};
  uint8_t data_[];
};

const BucketEntry* getIteratorEntry(BucketStorage::Allocation itr) {
  return reinterpret_cast<const BucketEntry*>(itr.view().data());
}
} // namespace

BufferView Bucket::Iterator::key() const {
  return getIteratorEntry(itr_)->key();
}

uint64_t Bucket::Iterator::keyHash() const {
  return getIteratorEntry(itr_)->keyHash();
}

BufferView Bucket::Iterator::value() const {
  return getIteratorEntry(itr_)->value();
}

bool Bucket::Iterator::keyEqualsTo(HashedKey hk) const {
  return getIteratorEntry(itr_)->keyEqualsTo(hk);
}

uint32_t Bucket::computeChecksum(BufferView view) {
  constexpr auto kChecksumStart = sizeof(checksum_);
  auto data = view.slice(kChecksumStart, view.size() - kChecksumStart);
  return navy::checksum(data);
}

Bucket& Bucket::initNew(MutableBufferView view, uint64_t generationTime) {
  return *new (view.data())
      Bucket(generationTime, view.size() - sizeof(Bucket));
}

BufferView Bucket::find(HashedKey hk) const {
  // TODO: (beyondsora) T31567959 store hash values in the beginning of
  //                              of a bucket for quick lookup
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(hk)) {
      return entry->value();
    }
    itr = storage_.getNext(itr);
  }
  return {};
}

uint32_t Bucket::insert(HashedKey hk,
                        BufferView value,
                        const DestructorCallback& destructorCb) {
  const auto size = BucketEntry::computeSize(hk.key().size(), value.size());
  XDCHECK_LE(size, storage_.capacity());

  const auto evictions = makeSpace(size, destructorCb);
  auto alloc = storage_.allocate(size);
  XDCHECK(!alloc.done());
  BucketEntry::create(alloc.view(), hk, value);

  return evictions;
}

uint32_t Bucket::makeSpace(uint32_t size,
                           const DestructorCallback& destructorCb) {
  const auto requiredSize = BucketStorage::slotSize(size);
  XDCHECK_LE(requiredSize, storage_.capacity());

  auto curFreeSpace = storage_.remainingCapacity();
  if (curFreeSpace >= requiredSize) {
    return 0;
  }

  uint32_t evictions = 0;
  auto itr = storage_.getFirst();
  while (true) {
    evictions++;

    if (destructorCb) {
      auto* entry = getIteratorEntry(itr);
      destructorCb(entry->key(), entry->value(), DestructorEvent::Recycled);
    }

    curFreeSpace += BucketStorage::slotSize(itr.view().size());
    if (curFreeSpace >= requiredSize) {
      storage_.removeUntil(itr);
      break;
    }
    itr = storage_.getNext(itr);
    XDCHECK(!itr.done());
  }
  return evictions;
}

uint32_t Bucket::remove(HashedKey hk, const DestructorCallback& destructorCb) {
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(hk)) {
      if (destructorCb) {
        destructorCb(entry->key(), entry->value(), DestructorEvent::Removed);
      }
      storage_.remove(itr);
      return 1;
    }
    itr = storage_.getNext(itr);
  }
  return 0;
}

Bucket::Iterator Bucket::getFirst() const {
  return Iterator{storage_.getFirst()};
}

Bucket::Iterator Bucket::getNext(Iterator itr) const {
  return Iterator{storage_.getNext(itr.itr_)};
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

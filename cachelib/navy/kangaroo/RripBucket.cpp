#include <vector>
#include <utility>

#include "cachelib/navy/kangaroo/RripBucket.h"
#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
static_assert(sizeof(RripBucket) == 24,
              "RripBucket overhead. If this changes, you may have to adjust the "
              "sizes used in unit tests.");

namespace {
// This maps to exactly how an entry is stored in a bucket on device.
class FOLLY_PACK_ATTR RripBucketEntry {
 public:
  static uint32_t computeSize(uint32_t keySize, uint32_t valueSize) {
    return sizeof(RripBucketEntry) + keySize + valueSize;
  }

  static RripBucketEntry& create(MutableBufferView storage,
                             HashedKey hk,
                             BufferView value) {
    new (storage.data()) RripBucketEntry{hk, value};
    return reinterpret_cast<RripBucketEntry&>(*storage.data());
  }

  BufferView key() const { return {keySize_, data_}; }

  bool keyEqualsTo(HashedKey hk) const {
    return hk == HashedKey::precomputed(key(), keyHash_);
  }
  
  bool keyEqualsTo(uint64_t keyHash) const {
    return keyHash == keyHash_;
  }

  uint64_t keyHash() const { return keyHash_; }

  BufferView value() const { return {valueSize_, data_ + keySize_}; }

 private:
  RripBucketEntry(HashedKey hk, BufferView value)
      : keySize_{static_cast<uint16_t>(hk.key().size())},
        valueSize_{static_cast<uint16_t>(value.size())},
        keyHash_{hk.keyHash()} {
    static_assert(sizeof(RripBucketEntry) == 12, "RripBucketEntry overhead");
    hk.key().copyTo(data_);
    value.copyTo(data_ + keySize_);
  }

  const uint16_t keySize_{};
  const uint16_t valueSize_{};
  const uint64_t keyHash_{};
  uint8_t data_[];
};

const RripBucketEntry* getIteratorEntry(RripBucketStorage::Allocation itr) {
  return reinterpret_cast<const RripBucketEntry*>(itr.view().data());
}
} // namespace

BufferView RripBucket::Iterator::key() const {
  return getIteratorEntry(itr_)->key();
}

uint64_t RripBucket::Iterator::keyHash() const {
  return getIteratorEntry(itr_)->keyHash();
}

BufferView RripBucket::Iterator::value() const {
  return getIteratorEntry(itr_)->value();
}

bool RripBucket::Iterator::keyEqualsTo(HashedKey hk) const {
  return getIteratorEntry(itr_)->keyEqualsTo(hk);
}

bool RripBucket::Iterator::keyEqualsTo(uint64_t keyHash) const {
  return getIteratorEntry(itr_)->keyEqualsTo(keyHash);
}

uint32_t RripBucket::computeChecksum(BufferView view) {
  constexpr auto kChecksumStart = sizeof(checksum_);
  auto data = view.slice(kChecksumStart, view.size() - kChecksumStart);
  return navy::checksum(data);
}

RripBucket& RripBucket::initNew(MutableBufferView view, uint64_t generationTime) {
  return *new (view.data())
      RripBucket(generationTime, view.size() - sizeof(RripBucket));
}

BufferView RripBucket::find(HashedKey hk, BitVectorUpdateVisitor addHitCallback) const {
  auto itr = storage_.getFirst();
  uint32_t keyIdx = 0;
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(hk)) {
      if (addHitCallback) {
        addHitCallback(keyIdx);
      }
      return entry->value();
    }
    itr = storage_.getNext(itr);
    keyIdx++;
  }
  return {};
}

/*Status RripBucket::findKey(uint64_t keyHash, HashedKey& hk) const {
  auto itr = storage_.getFirst();
  uint32_t keyIdx = 0;
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(keyHash)) {
      hk = HashedKey(entry->key());
      return Status::Ok;
    }
    itr = storage_.getNext(itr);
    keyIdx++;
  }
  return Status.NotFound;
}*/

uint32_t RripBucket::insert(HashedKey hk,
                        BufferView value,
                        uint8_t hits,
                        const DestructorCallback& destructorCb) {
  const auto size = RripBucketEntry::computeSize(hk.key().size(), value.size());
  XDCHECK_LE(size, storage_.capacity());

  const auto evictions = makeSpace(size, destructorCb);
  uint8_t rrip = getRripValue(hits);
  auto alloc = storage_.allocate(size, rrip);
  XDCHECK(!alloc.done());
  RripBucketEntry::create(alloc.view(), hk, value);

  return evictions;
}

bool RripBucket::isSpace(HashedKey hk, BufferView value, uint8_t hits) {
  const auto size = RripBucketEntry::computeSize(hk.key().size(), value.size());
  const auto requiredSize = RripBucketStorage::slotSize(size);
  XDCHECK_LE(requiredSize, storage_.capacity());

  auto curFreeSpace = storage_.remainingCapacity();
  uint8_t rrip = getRripValue(hits);

  auto itr = storage_.getFirst();
  while (curFreeSpace < requiredSize) {
    if (itr.done()) {
      return false;
    } else if (itr.rrip() < rrip) {
      return false;
    }
    curFreeSpace += RripBucketStorage::slotSize(itr.view().size());
    itr = storage_.getNext(itr);
  }
  return (curFreeSpace >= requiredSize);
}

uint32_t RripBucket::makeSpace(uint32_t size,
                           const DestructorCallback& destructorCb) {
  const auto requiredSize = RripBucketStorage::slotSize(size);
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

    curFreeSpace += RripBucketStorage::slotSize(itr.view().size());
    if (curFreeSpace >= requiredSize) {
      storage_.removeUntil(itr);
      break;
    }
    itr = storage_.getNext(itr);
    XDCHECK(!itr.done());
  }
  return evictions;
}

uint32_t RripBucket::remove(HashedKey hk, const DestructorCallback& destructorCb) {
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

void RripBucket::reorder(BitVectorReadVisitor isHitCallback) {
  uint32_t keyIdx = 0;
  Buffer firstMovedKey;
  bool movedKey = false;
  int8_t increment = -1;

  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    bool hit = isHitCallback(keyIdx);
    if (hit) {
      auto key = Buffer(entry->key());
      if (!movedKey) {
        movedKey = true;
        firstMovedKey = key.copy();
      } else if (firstMovedKey.view() == key.view()) {
        break;
      }

      auto value = Buffer(entry->value());
      HashedKey hk = HashedKey(key.view());
      BufferView valueView = value.view();
      itr = storage_.remove(itr);
      const auto size = RripBucketEntry::computeSize(hk.key().size(), valueView.size());
      // promotion rrip so a hit promotes to 0
      auto alloc = storage_.allocate(size, 0);
      RripBucketEntry::create(alloc.view(), hk, valueView);
    } else {
      if (increment == -1) {
        increment = (int8_t) getIncrement(itr.rrip());
      }
      storage_.incrementRrip(itr, increment);
      itr = storage_.getNext(itr);
    }

    keyIdx++;
  }
}

RripBucket::Iterator RripBucket::getFirst() const {
  return Iterator{storage_.getFirst()};
}

RripBucket::Iterator RripBucket::getNext(Iterator itr) const {
  return Iterator{storage_.getNext(itr.itr_)};
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

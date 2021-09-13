#include <vector>
#include <utility>

#include "cachelib/navy/kangaroo/LogBucket.h"
#include "cachelib/navy/common/Hash.h"

namespace facebook {
namespace cachelib {
namespace navy {
static_assert(sizeof(LogBucket) == 24,
              "LogBucket overhead. If this changes, you may have to adjust the "
              "sizes used in unit tests.");

namespace {
// This maps to exactly how an entry is stored in a bucket on device.
class FOLLY_PACK_ATTR LogBucketEntry {
 public:
  static uint32_t computeSize(uint32_t keySize, uint32_t valueSize) {
    return sizeof(LogBucketEntry) + keySize + valueSize;
  }

  static LogBucketEntry& create(MutableBufferView storage,
                             HashedKey hk,
                             BufferView value) {
    new (storage.data()) LogBucketEntry{hk, value};
    return reinterpret_cast<LogBucketEntry&>(*storage.data());
  }

  BufferView key() const { return {keySize_, data_}; }
  
  uint64_t keyHash() const { return makeHK(key()).keyHash(); }

  bool keyEqualsTo(HashedKey hk) const {
    return hk == makeHK(key());
  }
  
  bool keyEqualsTo(uint64_t hash) const {
    return hash == keyHash();
  }

  BufferView value() const { return {valueSize_, data_ + keySize_}; }

 private:
  LogBucketEntry(HashedKey hk, BufferView value)
      : keySize_{static_cast<uint16_t>(hk.key().size())},
        valueSize_{static_cast<uint16_t>(value.size())} {
    static_assert(sizeof(LogBucketEntry) == 4, "LogBucketEntry overhead");
    hk.key().copyTo(data_);
    value.copyTo(data_ + keySize_);
  }

  const uint16_t keySize_{};
  const uint16_t valueSize_{};
  uint8_t data_[];
};

const LogBucketEntry* getIteratorEntry(KangarooBucketStorage::Allocation itr) {
  return reinterpret_cast<const LogBucketEntry*>(itr.view().data());
}
} // namespace

BufferView LogBucket::Iterator::key() const {
  return getIteratorEntry(itr_)->key();
}

uint64_t LogBucket::Iterator::keyHash() const {
  return getIteratorEntry(itr_)->keyHash();
}

BufferView LogBucket::Iterator::value() const {
  return getIteratorEntry(itr_)->value();
}

bool LogBucket::Iterator::keyEqualsTo(HashedKey hk) const {
  return getIteratorEntry(itr_)->keyEqualsTo(hk);
}

bool LogBucket::Iterator::keyEqualsTo(uint64_t keyHash) const {
  return getIteratorEntry(itr_)->keyEqualsTo(keyHash);
}

uint32_t LogBucket::computeChecksum(BufferView view) {
  constexpr auto kChecksumStart = sizeof(checksum_);
  auto data = view.slice(kChecksumStart, view.size() - kChecksumStart);
  return navy::checksum(data);
}

LogBucket& LogBucket::initNew(MutableBufferView view, uint64_t generationTime) {
  return *new (view.data())
      LogBucket(generationTime, view.size() - sizeof(LogBucket));
}

BufferView LogBucket::find(HashedKey hk) const {
  auto itr = storage_.getFirst();
  uint32_t keyIdx = 0;
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(hk)) {
      return entry->value();
    }
    itr = storage_.getNext(itr);
    keyIdx++;
  }
  return {};
}

BufferView LogBucket::findTag(uint32_t tag, HashedKey& hk) const {
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    hk = makeHK(entry->key());
    if (createTag(hk) == tag) {
      return entry->value();
    }
    itr = storage_.getNext(itr);
  }
  return {};
}

uint32_t LogBucket::insert(HashedKey hk,
                        BufferView value,
                        const DestructorCallback& destructorCb) {
  const auto size = LogBucketEntry::computeSize(hk.key().size(), value.size());
  XDCHECK_LE(size, storage_.capacity());

  const auto evictions = makeSpace(size, destructorCb);
  auto alloc = storage_.allocate(size);
  XDCHECK(!alloc.done());
  LogBucketEntry::create(alloc.view(), hk, value);

  return evictions;
}

void LogBucket::insert(KangarooBucketStorage::Allocation alloc,
                        HashedKey hk,
                        BufferView value) {
  XDCHECK(!alloc.done());
  LogBucketEntry::create(alloc.view(), hk, value);
}

KangarooBucketStorage::Allocation LogBucket::allocate(HashedKey hk,
    BufferView value) {
  const auto size = LogBucketEntry::computeSize(hk.key().size(), value.size());
  XDCHECK_LE(size, storage_.remainingCapacity());

  auto alloc = storage_.allocate(size);
  XDCHECK(!alloc.done());
  return alloc;
}

void LogBucket::clear() {
  storage_.clear();
}

bool LogBucket::isSpace(HashedKey hk, BufferView value) {
  const auto size = LogBucketEntry::computeSize(hk.key().size(), value.size());
  const auto requiredSize = KangarooBucketStorage::slotSize(size);
  XDCHECK_LE(requiredSize, storage_.capacity());

  auto curFreeSpace = storage_.remainingCapacity();
  return (curFreeSpace >= requiredSize);
}

uint32_t LogBucket::makeSpace(uint32_t size,
                           const DestructorCallback& destructorCb) {
  const auto requiredSize = KangarooBucketStorage::slotSize(size);
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

    curFreeSpace += KangarooBucketStorage::slotSize(itr.view().size());
    if (curFreeSpace >= requiredSize) {
      storage_.removeUntil(itr);
      break;
    }
    itr = storage_.getNext(itr);
    XDCHECK(!itr.done());
  }
  return evictions;
}

uint32_t LogBucket::remove(HashedKey hk, const DestructorCallback& destructorCb) {
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

void LogBucket::reorder(BitVectorReadVisitor isHitCallback) {
  uint32_t keyIdx = 0;
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    bool hit = isHitCallback(keyIdx);
    if (hit) {
      auto key = Buffer(entry->key());
      auto value = Buffer(entry->value());
      HashedKey hk = HashedKey(key.view());
      BufferView valueView = value.view();
      storage_.remove(itr);
      const auto size = LogBucketEntry::computeSize(hk.key().size(), valueView.size());
      auto alloc = storage_.allocate(size);
      LogBucketEntry::create(alloc.view(), hk, valueView);
    }

    keyIdx++;
    itr = storage_.getNext(itr);
  }
}

LogBucket::Iterator LogBucket::getFirst() const {
  return Iterator{storage_.getFirst()};
}

LogBucket::Iterator LogBucket::getNext(Iterator itr) const {
  return Iterator{storage_.getNext(itr.itr_)};
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

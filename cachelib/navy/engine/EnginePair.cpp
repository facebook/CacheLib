// (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

#include "cachelib/navy/engine/EnginePair.h"

namespace facebook {
namespace cachelib {
namespace navy {

EnginePair::EnginePair(std::unique_ptr<Engine> smallItemCache,
                       std::unique_ptr<Engine> largeItemCache,
                       uint32_t smallItemMaxSize,
                       JobScheduler* scheduler)
    : smallItemMaxSize_(smallItemCache ? smallItemMaxSize : 0),
      largeItemCache_{std::move(largeItemCache)},
      smallItemCache_{std::move(smallItemCache)},
      scheduler_(scheduler) {}

bool EnginePair::isItemLarge(HashedKey key, BufferView value) const {
  return key.key().size() + value.size() > smallItemMaxSize_;
}

std::pair<Engine&, Engine&> EnginePair::select(HashedKey key,
                                               BufferView value) const {
  if (isItemLarge(key, value)) {
    return {*largeItemCache_, *smallItemCache_};
  } else {
    return {*smallItemCache_, *largeItemCache_};
  }
}

bool EnginePair::couldExist(HashedKey key) const {
  bool couldExist =
      smallItemCache_->couldExist(key) || largeItemCache_->couldExist(key);
  if (!couldExist) {
    lookupCount_.inc();
  }
  return couldExist;
}

Status EnginePair::lookupSync(HashedKey hk, Buffer& value) const {
  lookupCount_.inc();
  Status status{Status::NotFound};
  // We do busy wait because we don't expect many retries.
  while ((status = largeItemCache_->lookup(hk, value)) == Status::Retry) {
    std::this_thread::yield();
  }
  if (status == Status::NotFound) {
    while ((status = smallItemCache_->lookup(hk, value)) == Status::Retry) {
      std::this_thread::yield();
    }
  }
  updateLookupStats(status);
  return status;
}

Status EnginePair::insertInternal(HashedKey hk,
                                  BufferView value,
                                  bool& skipInsertion) {
  auto selection = select(hk, value);
  Status status = Status::Ok;
  if (!skipInsertion) {
    status = selection.first.insert(hk, value);
    if (status == Status::Retry) {
      return status;
    }
    skipInsertion = true;
  }
  if (status != Status::DeviceError) {
    auto rs = selection.second.remove(hk);
    if (rs == Status::Retry) {
      return rs;
    }
    if (rs != Status::Ok && rs != Status::NotFound) {
      XLOGF(ERR, "Insert failed to remove other: {}", toString(rs));
      status = Status::BadState;
    }
  }

  switch (status) {
  case Status::Ok:
    succInsertCount_.inc();
    break;
  case Status::BadState:
  case Status::DeviceError:
    ioErrorCount_.inc();
    break;
  default:;
  };

  return status;
}

void EnginePair::scheduleInsert(HashedKey hk,
                                BufferView value,
                                InsertCallback cb) {
  insertCount_.inc();
  scheduler_->enqueueWithKey(
      [this, cb = std::move(cb), hk, value, skipInsertion = false]() mutable {
        auto status = insertInternal(hk, value, skipInsertion);
        if (status == Status::Retry) {
          return JobExitCode::Reschedule;
        }

        if (cb) {
          cb(status, hk);
        }

        return JobExitCode::Done;
      },
      "insert",
      JobType::Write,
      hk.keyHash());
}

void EnginePair::updateLookupStats(Status status) const {
  switch (status) {
  case Status::Ok:
    succLookupCount_.inc();
    break;
  case Status::DeviceError:
    ioErrorCount_.inc();
    break;
  default:;
  }
}

Status EnginePair::lookupInternal(HashedKey hk,
                                  Buffer& value,
                                  bool& skipLargeItemCache) const {
  Status status{Status::NotFound};
  if (!skipLargeItemCache) {
    status = largeItemCache_->lookup(hk, value);
    if (status == Status::Retry) {
      return status;
    }
    skipLargeItemCache = true;
  }
  if (status == Status::NotFound) {
    status = smallItemCache_->lookup(hk, value);
    if (status == Status::Retry) {
      return status;
    }
  }
  updateLookupStats(status);
  return status;
}

void EnginePair::scheduleLookup(HashedKey hk, LookupCallback cb) {
  scheduler_->enqueueWithKey(
      [this, cb = std::move(cb), hk, skipLargeItemCache = false]() mutable {
        Buffer value;
        Status status = lookupInternal(hk, value, skipLargeItemCache);
        if (status == Status::Retry) {
          return JobExitCode::Reschedule;
        }
        if (cb) {
          cb(status, hk, std::move(value));
        }

        return JobExitCode::Done;
      },
      "lookup",
      JobType::Read,
      hk.keyHash());
}

Status EnginePair::removeSync(HashedKey hk) {
  Status status{Status::Ok};
  bool skipSmallItemCache = false;
  while ((status = removeHashedKeyInternal(hk, skipSmallItemCache)) ==
         Status::Retry) {
    std::this_thread::yield();
  }
  return status;
}

Status EnginePair::removeHashedKeyInternal(HashedKey hk,
                                           bool& skipSmallItemCache) {
  removeCount_.inc();
  Status status = Status::NotFound;
  if (!skipSmallItemCache) {
    status = smallItemCache_->remove(hk);
  }
  if (status == Status::NotFound) {
    status = largeItemCache_->remove(hk);
    skipSmallItemCache = true;
  }
  switch (status) {
  case Status::Ok:
    succRemoveCount_.inc();
    break;
  case Status::DeviceError:
    ioErrorCount_.inc();
    break;
  default:;
  }
  return status;
}

void EnginePair::scheduleRemove(HashedKey hk, RemoveCallback cb) {
  scheduler_->enqueueWithKey(
      [this,
       cb = std::move(cb),
       hk = hk,
       skipSmallItemCache = false]() mutable {
        auto status = removeHashedKeyInternal(hk, skipSmallItemCache);
        if (status == Status::Retry) {
          return JobExitCode::Reschedule;
        }
        if (cb) {
          cb(status, hk);
        }
        return JobExitCode::Done;
      },
      "remove",
      JobType::Write,
      hk.keyHash());
}

void EnginePair::flush() {
  smallItemCache_->flush();
  largeItemCache_->flush();
}

void EnginePair::reset() {
  smallItemCache_->reset();
  largeItemCache_->reset();
}

// persist the navy engines state
void EnginePair::persist(RecordWriter& rw) const {
  largeItemCache_->persist(rw);
  smallItemCache_->persist(rw);
}

// recover the navy engines state
bool EnginePair::recover(RecordReader& rr) {
  return largeItemCache_->recover(rr) && smallItemCache_->recover(rr);
}

void EnginePair::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_inserts", insertCount_.get());
  visitor("navy_succ_inserts", succInsertCount_.get());
  visitor("navy_lookups", lookupCount_.get());
  visitor("navy_succ_lookups", succLookupCount_.get());
  visitor("navy_removes", removeCount_.get());
  visitor("navy_succ_removes", succRemoveCount_.get());
  visitor("navy_io_errors", ioErrorCount_.get());
  visitor("navy_total_usable_size", getUsableSize());
  largeItemCache_->getCounters(visitor);
  smallItemCache_->getCounters(visitor);
}

uint64_t EnginePair::getUsableSize() const {
  return largeItemCache_->getSize() + smallItemCache_->getSize();
}

std::pair<Status, std::string> EnginePair::getRandomAlloc(Buffer& value) {
  static uint64_t largeCacheSize = largeItemCache_->getSize();
  static uint64_t smallCacheSize = smallItemCache_->getSize();

  bool fromLargeItemCache =
      folly::Random::rand64(0, largeCacheSize + smallCacheSize) >=
      smallCacheSize;

  if (fromLargeItemCache) {
    return largeItemCache_->getRandomAlloc(value);
  }

  return smallItemCache_->getRandomAlloc(value);
}

} // namespace navy
} // namespace cachelib

} // namespace facebook

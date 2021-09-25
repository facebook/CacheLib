/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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

#include "cachelib/navy/driver/Driver.h"

#include <folly/synchronization/Baton.h>

#include "cachelib/navy/admission_policy/DynamicRandomAP.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/driver/NoopEngine.h"
#include "folly/Format.h"

namespace facebook {
namespace cachelib {
namespace navy {
Driver::Config& Driver::Config::validate() {
  if (smallItemCache != nullptr && smallItemMaxSize == 0) {
    throw std::invalid_argument("invalid small item cache params");
  }
  if (smallItemCache != nullptr) {
    if (smallItemMaxSize > smallItemCache->getMaxItemSize()) {
      throw std::invalid_argument(folly::sformat(
          "small item max size should not excceed: {}, but is set to be: {}",
          smallItemCache->getMaxItemSize(), smallItemMaxSize));
    }
  }
  return *this;
}

Driver::Driver(Config&& config)
    : Driver{std::move(config.validate()), ValidConfigTag{}} {}

Driver::Driver(Config&& config, ValidConfigTag)
    : smallItemMaxSize_{config.smallItemCache ? config.smallItemMaxSize : 0},
      maxConcurrentInserts_{config.maxConcurrentInserts},
      maxParcelMemory_{config.maxParcelMemory},
      metadataSize_{config.metadataSize},
      device_{std::move(config.device)},
      scheduler_{std::move(config.scheduler)},
      largeItemCache_{std::move(config.largeItemCache)},
      smallItemCache_{std::move(config.smallItemCache)},
      admissionPolicy_{std::move(config.admissionPolicy)} {
  if (!largeItemCache_) {
    XLOG(INFO, "Large item cache is noop");
    largeItemCache_ = std::make_unique<NoopEngine>();
  }
  if (!smallItemCache_) {
    XLOG(INFO, "Small item cache is noop");
    smallItemCache_ = std::make_unique<NoopEngine>();
  }
  XLOGF(INFO, "Max concurrent inserts: {}", maxConcurrentInserts_);
  XLOGF(INFO, "Max parcel memory: {}", maxParcelMemory_);
}

Driver::~Driver() {
  XLOG(INFO, "Driver: finish scheduler");
  scheduler_->finish();
  XLOG(INFO, "Driver: finish scheduler successful");
  // Destroy this for safety first
  scheduler_.reset();
}

std::pair<Engine&, Engine&> Driver::select(BufferView key,
                                           BufferView value) const {
  if (key.size() + value.size() < smallItemMaxSize_) {
    return {*smallItemCache_, *largeItemCache_};
  } else {
    return {*largeItemCache_, *smallItemCache_};
  }
}

bool Driver::couldExist(BufferView key) {
  const HashedKey hk{key};
  auto couldExist =
      smallItemCache_->couldExist(hk) || largeItemCache_->couldExist(hk);
  if (!couldExist) {
    lookupCount_.inc();
  }
  return couldExist;
}

Status Driver::insert(BufferView key, BufferView value) {
  folly::Baton<> done;
  Status cbStatus{Status::Ok};
  auto status = insertAsync(key, value,
                            [&done, &cbStatus](Status s, BufferView /* key */) {
                              cbStatus = s;
                              done.post();
                            });
  if (status != Status::Ok) {
    return status;
  }
  done.wait();
  return cbStatus;
}

bool Driver::admissionTest(HashedKey hk, BufferView value) const {
  // If this parcel makes our memory above the limit, we reject it and
  // revert back increment we made. We can't split check and increment!
  // We can't check value before - it will over admit things. Same with
  // concurrent inserts.
  size_t parcelSize = hk.key().size() + value.size();
  auto currParcelMemory = parcelMemory_.add_fetch(parcelSize);
  auto currConcurrentInserts = concurrentInserts_.add_fetch(1);

  if (!admissionPolicy_ || admissionPolicy_->accept(hk, value)) {
    if (currConcurrentInserts <= maxConcurrentInserts_) {
      if (currParcelMemory <= maxParcelMemory_) {
        acceptedCount_.inc();
        acceptedBytes_.add(parcelSize);
        return true;
      } else {
        rejectedParcelMemoryCount_.inc();
      }
    } else {
      rejectedConcurrentInsertsCount_.inc();
    }
  }
  rejectedCount_.inc();
  rejectedBytes_.add(parcelSize);

  // Revert counter modifications. Remember, can't assign back atomic.
  concurrentInserts_.dec();
  parcelMemory_.sub(parcelSize);

  return false;
}

Status Driver::insertAsync(BufferView key,
                           BufferView value,
                           InsertCallback cb) {
  insertCount_.inc();

  const HashedKey hk{key};
  if (key.size() > kMaxKeySize) {
    rejectedCount_.inc();
    rejectedBytes_.add(hk.key().size() + value.size());
    return Status::Rejected;
  }

  if (!admissionTest(hk, value)) {
    return Status::Rejected;
  }

  scheduler_->enqueueWithKey(
      [this, cb = std::move(cb), hk, value]() mutable {
        auto selection = select(hk.key(), value);
        auto status = selection.first.insert(hk, value);
        if (status == Status::Retry) {
          return JobExitCode::Reschedule;
        }
        if (status != Status::DeviceError) {
          auto rs = selection.second.remove(hk);
          XDCHECK_NE(rs, Status::Retry);
          if (rs != Status::Ok && rs != Status::NotFound) {
            XLOGF(ERR, "Insert failed to remove other: {}", toString(rs));
            status = Status::BadState;
          }
        }

        if (cb) {
          cb(status, hk.key());
        }
        parcelMemory_.sub(hk.key().size() + value.size());
        concurrentInserts_.dec();

        switch (status) {
        case Status::Ok:
          succInsertCount_.inc();
          break;
        case Status::BadState:
        case Status::DeviceError:
          ioErrorCount_.inc();
          break;
        default:;
        }
        return JobExitCode::Done;
      },
      "insert",
      JobType::Write,
      hk.keyHash());
  return Status::Ok;
}

void Driver::updateLookupStats(Status status) const {
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

Status Driver::lookup(BufferView key, Buffer& value) {
  // We do busy wait because we don't expect many retries.
  lookupCount_.inc();
  const HashedKey hk{key};
  Status status{Status::NotFound};
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

Status Driver::lookupAsync(BufferView key, LookupCallback cb) {
  lookupCount_.inc();
  const HashedKey hk{key};
  XDCHECK(cb);

  scheduler_->enqueueWithKey(
      [this, cb = std::move(cb), hk, skipLargeItemCache = false]() mutable {
        Buffer value;
        Status status{Status::NotFound};
        if (!skipLargeItemCache) {
          status = largeItemCache_->lookup(hk, value);
          if (status == Status::Retry) {
            return JobExitCode::Reschedule;
          }
          skipLargeItemCache = true;
        }
        if (status == Status::NotFound) {
          status = smallItemCache_->lookup(hk, value);
          if (status == Status::Retry) {
            return JobExitCode::Reschedule;
          }
        }

        if (cb) {
          cb(status, hk.key(), std::move(value));
        }

        updateLookupStats(status);
        return JobExitCode::Done;
      },
      "lookup",
      JobType::Read,
      hk.keyHash());
  return Status::Ok;
}

Status Driver::removeHashedKey(HashedKey hk) {
  removeCount_.inc();
  auto status = smallItemCache_->remove(hk);
  XDCHECK_NE(status, Status::Retry);
  if (status == Status::NotFound) {
    status = largeItemCache_->remove(hk);
    // This assert knows that BlockCache (our implementation) never
    // returns retry. Otherwise, we have to do something with retry.
    XDCHECK_NE(status, Status::Retry);
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

Status Driver::remove(BufferView key) { return removeHashedKey(makeHK(key)); }

Status Driver::removeAsync(BufferView key, RemoveCallback cb) {
  const HashedKey hk{key};
  scheduler_->enqueueWithKey(
      [this, cb = std::move(cb), hk]() mutable {
        auto status = removeHashedKey(hk);
        if (cb) {
          cb(status, hk.key());
        }
        return JobExitCode::Done;
      },
      "remove",
      JobType::Write,
      hk.keyHash());
  return Status::Ok;
}

void Driver::flush() {
  scheduler_->finish();
  smallItemCache_->flush();
  largeItemCache_->flush();
}

void Driver::reset() {
  XLOG(INFO, "Reset Navy");
  scheduler_->finish();
  smallItemCache_->reset();
  largeItemCache_->reset();
  if (admissionPolicy_) {
    admissionPolicy_->reset();
  }
}

void Driver::persist() const {
  auto rw = createMetadataRecordWriter(*device_, metadataSize_);
  if (rw) {
    largeItemCache_->persist(*rw);
    smallItemCache_->persist(*rw);
  }
}

bool Driver::recover() {
  auto rr = createMetadataRecordReader(*device_, metadataSize_);
  if (!rr) {
    return false;
  }
  if (rr->isEnd()) {
    return false;
  }
  // Because we insert item and remove from the other engine, partial recovery
  // is potentially possible.
  bool recovered =
      largeItemCache_->recover(*rr) && smallItemCache_->recover(*rr);
  if (!recovered) {
    reset();
  }
  if (recovered) {
    // If recovery is successful, invalidate the metadata
    auto rw = createMetadataRecordWriter(*device_, metadataSize_);
    if (rw) {
      return rw->invalidate();
    } else {
      recovered = false;
    }
  }
  return recovered;
}

bool Driver::updateMaxRateForDynamicRandomAP(uint64_t maxRate) {
  DynamicRandomAP* ptr = dynamic_cast<DynamicRandomAP*>(admissionPolicy_.get());
  if (ptr) {
    ptr->setMaxWriteRate(maxRate);
    return true;
  }
  return false;
}

uint64_t Driver::getSize() const { return device_->getSize(); }

void Driver::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_inserts", insertCount_.get());
  visitor("navy_succ_inserts", succInsertCount_.get());
  visitor("navy_lookups", lookupCount_.get());
  visitor("navy_succ_lookups", succLookupCount_.get());
  visitor("navy_removes", removeCount_.get());
  visitor("navy_succ_removes", succRemoveCount_.get());
  visitor("navy_rejected", rejectedCount_.get());
  visitor("navy_rejected_concurrent_inserts",
          rejectedConcurrentInsertsCount_.get());
  visitor("navy_rejected_parcel_memory", rejectedParcelMemoryCount_.get());
  visitor("navy_rejected_bytes", rejectedBytes_.get());
  visitor("navy_accepted_bytes", acceptedBytes_.get());
  visitor("navy_accepted", acceptedCount_.get());
  visitor("navy_io_errors", ioErrorCount_.get());
  visitor("navy_parcel_memory", parcelMemory_.get());
  visitor("navy_concurrent_inserts", concurrentInserts_.get());
  scheduler_->getCounters(visitor);
  largeItemCache_->getCounters(visitor);
  smallItemCache_->getCounters(visitor);
  if (admissionPolicy_) {
    admissionPolicy_->getCounters(visitor);
  }
  // Can be nullptr in driver tests
  if (device_) {
    device_->getCounters(visitor);
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

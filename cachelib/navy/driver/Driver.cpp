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

#include "cachelib/navy/driver/Driver.h"

#include <folly/Range.h>
#include <folly/fibers/Baton.h>

#include "cachelib/common/Serialization.h"
#include "cachelib/navy/admission_policy/DynamicRandomAP.h"
#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook::cachelib::navy {
namespace {
// get discrete_distribution based on enginePair sizes.
std::discrete_distribution<size_t> getDist(
    const std::vector<EnginePair>& enginePairs) {
  std::vector<size_t> sizes;
  sizes.reserve(enginePairs.size());
  for (auto& p : enginePairs) {
    sizes.push_back(p.getUsableSize());
  }
  return std::discrete_distribution<size_t>(sizes.begin(), sizes.end());
}
} // namespace

Driver::Config& Driver::Config::validate() {
  if (enginePairs.empty()) {
    throw std::invalid_argument("There should be at least one engine pair.");
  }

  for (auto& p : enginePairs) {
    p.validate();
  }
  if (enginePairs.size() > 1 && (!selector)) {
    throw std::invalid_argument("More than one engine pairs with no selector.");
  }
  return *this;
}

Driver::Driver(Config&& config)
    : Driver{std::move(config.validate()), ValidConfigTag{}} {}

Driver::Driver(Config&& config, ValidConfigTag)
    : maxConcurrentInserts_{config.maxConcurrentInserts},
      maxParcelMemory_{config.maxParcelMemory},
      metadataSize_{config.metadataSize},
      maxKeySize_{config.maxKeySize},
      useEstimatedWriteSize_{config.useEstimatedWriteSize},
      device_{std::move(config.device)},
      scheduler_{std::move(config.scheduler)},
      selector_{std::move(config.selector)},
      enginePairs_{std::move(config.enginePairs)},
      admissionPolicy_{std::move(config.admissionPolicy)},
      rejectedCountByEngine_{enginePairs_.size() > 1 ? enginePairs_.size() : 0},
      rejectedConcurrentInsertsCountByEngine_{
          enginePairs_.size() > 1 ? enginePairs_.size() : 0},
      rejectedParcelMemoryCountByEngine_{
          enginePairs_.size() > 1 ? enginePairs_.size() : 0},
      rejectedBytesByEngine_{enginePairs_.size() > 1 ? enginePairs_.size()
                                                     : 0} {
  getRandomAllocDist = getDist(enginePairs_);
  XLOGF(INFO, "Max concurrent inserts: {}", maxConcurrentInserts_);
  XLOGF(INFO, "Max parcel memory: {}", maxParcelMemory_);
  XLOGF(INFO, "Use Write Estimated Size: {}", useEstimatedWriteSize_);
}

Driver::~Driver() {
  XLOG(INFO, "Driver: finish scheduler");
  drain();
  XLOG(INFO, "Driver: finish scheduler successful");
  // Destroy this for safety first
  scheduler_.reset();
}

size_t Driver::selectEnginePair(HashedKey hk) const {
  if (selector_) {
    return selector_(hk);
  } else {
    return 0;
  }
}

bool Driver::isItemLarge(HashedKey key, BufferView value) const {
  return enginePairs_[selectEnginePair(key)].isItemLarge(key, value);
}

bool Driver::couldExist(HashedKey hk) {
  return enginePairs_[selectEnginePair(hk)].couldExist(hk);
}

uint64_t Driver::estimateWriteSize(HashedKey hk, BufferView value) const {
  return useEstimatedWriteSize_
             ? enginePairs_[selectEnginePair(hk)].estimateWriteSize(hk, value)
             : hk.key().size() + value.size();
}

Status Driver::insert(HashedKey key, BufferView value) {
  folly::fibers::Baton done;
  Status cbStatus{Status::Ok};
  auto status = insertAsync(key, value,
                            [&done, &cbStatus](Status s, HashedKey /* key */) {
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
  auto enginePairIndex = selectEnginePair(hk);

  if (!admissionPolicy_ ||
      admissionPolicy_->accept(hk, value, estimateWriteSize(hk, value))) {
    if (currConcurrentInserts <= maxConcurrentInserts_) {
      if (currParcelMemory <= maxParcelMemory_) {
        acceptedCount_.inc();
        acceptedBytes_.add(parcelSize);
        return true;
      } else {
        rejectedParcelMemoryCount_.inc();
        if (rejectedParcelMemoryCountByEngine_.size() > enginePairIndex) {
          rejectedParcelMemoryCountByEngine_[enginePairIndex].inc();
        }
      }
    } else {
      rejectedConcurrentInsertsCount_.inc();
      if (rejectedConcurrentInsertsCountByEngine_.size() > enginePairIndex) {
        rejectedConcurrentInsertsCountByEngine_[enginePairIndex].inc();
      }
    }
  }
  rejectedCount_.inc();
  rejectedBytes_.add(parcelSize);
  if (rejectedCountByEngine_.size() > enginePairIndex) {
    rejectedCountByEngine_[enginePairIndex].inc();
  }
  if (rejectedBytesByEngine_.size() > enginePairIndex) {
    rejectedBytesByEngine_[enginePairIndex].add(parcelSize);
  }

  // Revert counter modifications. Remember, can't assign back atomic.
  concurrentInserts_.dec();
  parcelMemory_.sub(parcelSize);

  return false;
}

Status Driver::insertAsync(HashedKey hk, BufferView value, InsertCallback cb) {
  if (hk.key().size() > maxKeySize_) {
    rejectedCount_.inc();
    rejectedBytes_.add(hk.key().size() + value.size());

    auto enginePairIndex = selectEnginePair(hk);
    if (rejectedCountByEngine_.size() > enginePairIndex) {
      rejectedCountByEngine_[enginePairIndex].inc();
    }
    if (rejectedBytesByEngine_.size() > enginePairIndex) {
      rejectedBytesByEngine_[enginePairIndex].add(hk.key().size() +
                                                  value.size());
    }
    return Status::Rejected;
  }

  if (!admissionTest(hk, value)) {
    return Status::Rejected;
  }

  enginePairs_[selectEnginePair(hk)].scheduleInsert(
      hk, value,
      [this, totalSize = hk.key().size() + value.size(),
       cb = std::move(cb)](Status s, HashedKey hashedKey) mutable {
        if (cb) {
          cb(s, hashedKey);
        }
        parcelMemory_.sub(totalSize);
        concurrentInserts_.dec();
      });
  return Status::Ok;
}

Status Driver::lookup(HashedKey hk, Buffer& value) {
  return enginePairs_[selectEnginePair(hk)].lookupSync(hk, value);
}

void Driver::lookupAsync(HashedKey hk, LookupCallback cb) {
  XDCHECK(cb);
  enginePairs_[selectEnginePair(hk)].scheduleLookup(hk, std::move(cb));
}

Status Driver::remove(HashedKey hk) {
  return enginePairs_[selectEnginePair(hk)].removeSync(hk);
}

void Driver::removeAsync(HashedKey hk, RemoveCallback cb) {
  enginePairs_[selectEnginePair(hk)].scheduleRemove(hk, std::move(cb));
}

void Driver::drain() {
  scheduler_->finish();
  for (size_t idx = 0; idx < enginePairs_.size(); idx++) {
    enginePairs_[idx].drain();
  }
}

void Driver::flush() {
  drain(); // Flush all pending jobs
  for (size_t idx = 0; idx < enginePairs_.size(); idx++) {
    enginePairs_[idx].flush();
  }
}

void Driver::reset() {
  XLOG(INFO, "Reset Navy");
  drain();
  for (size_t idx = 0; idx < enginePairs_.size(); idx++) {
    enginePairs_[idx].reset();
  }
  if (admissionPolicy_) {
    admissionPolicy_->reset();
  }
}

void Driver::persist() const {
  auto rw = createMetadataRecordWriter(*device_, metadataSize_);
  if (rw) {
    for (size_t idx = 0; idx < enginePairs_.size(); idx++) {
      enginePairs_[idx].persist(*rw);
    }
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
  bool recovered = true;
  for (size_t idx = 0; idx < enginePairs_.size(); idx++) {
    recovered &= enginePairs_[idx].recover(*rr);
    if (!recovered) {
      break;
    }
  }

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

uint64_t Driver::getUsableSize() const {
  uint64_t size = 0;
  for (size_t idx = 0; idx < enginePairs_.size(); idx++) {
    size += enginePairs_[idx].getUsableSize();
  }
  return size;
}

void Driver::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_rejected", rejectedCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_rejected_concurrent_inserts",
          rejectedConcurrentInsertsCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_rejected_parcel_memory", rejectedParcelMemoryCount_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_rejected_bytes", rejectedBytes_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_accepted_bytes", acceptedBytes_.get(),
          CounterVisitor::CounterType::RATE);
  visitor("navy_accepted", acceptedCount_.get(),
          CounterVisitor::CounterType::RATE);

  visitor("navy_parcel_memory", parcelMemory_.get());
  visitor("navy_concurrent_inserts", concurrentInserts_.get());

  scheduler_->getCounters(visitor);
  if (enginePairs_.size() > 1) {
    for (size_t idx = 0; idx < enginePairs_.size(); idx++) {
      auto suffix =
          enginePairs_[idx].getName().empty()
              ? ""
              : folly::to<std::string>(":", enginePairs_[idx].getName());

      const CounterVisitor pv{[&visitor, idx,
                               &suffix](folly::StringPiece name, double count,
                                        CounterVisitor::CounterType type) {
        visitor(folly::to<std::string>(name, "_", idx, suffix), count, type);
      }};
      enginePairs_[idx].getCounters(pv);

      visitor(folly::to<std::string>("navy_rejected_", idx, suffix),
              rejectedCountByEngine_[idx].get(),
              CounterVisitor::CounterType::RATE);
      visitor(folly::to<std::string>("navy_rejected_concurrent_inserts_", idx,
                                     suffix),
              rejectedConcurrentInsertsCountByEngine_[idx].get(),
              CounterVisitor::CounterType::RATE);
      visitor(
          folly::to<std::string>("navy_rejected_parcel_memory_", idx, suffix),
          rejectedParcelMemoryCountByEngine_[idx].get(),
          CounterVisitor::CounterType::RATE);
      visitor(folly::to<std::string>("navy_rejected_bytes_", idx, suffix),
              rejectedBytesByEngine_[idx].get(),
              CounterVisitor::CounterType::RATE);
    }
    visitor("navy_total_usable_size", getUsableSize());
  } else {
    enginePairs_[0].getCounters(visitor);
  }

  if (admissionPolicy_) {
    admissionPolicy_->getCounters(visitor);
  }
  // Can be nullptr in driver tests
  if (device_) {
    device_->getCounters(visitor);
  }
}

std::pair<Status, std::string> Driver::getRandomAlloc(Buffer& value) {
  size_t idx = getRandomAllocDist(getRandomAllocGen);
  return enginePairs_[idx].getRandomAlloc(value);
}

void Driver::updateEvictionStats(HashedKey key,
                                 BufferView value,
                                 uint32_t lifetime) {
  enginePairs_[selectEnginePair(key)].updateEvictionStats(key, value, lifetime);
}
} // namespace facebook::cachelib::navy

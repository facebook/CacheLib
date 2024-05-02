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

#include <folly/File.h>
#include <folly/MPMCQueue.h>

#include <cstdint>
#include <string>
#include <thread>

#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/common/Time.h"
#include "cachelib/navy/serialization/RecordIO.h"
#include "cachelib/object_cache/persistence/Serialization.h"
#include "cachelib/object_cache/persistence/gen-cpp2/persistent_data_types.h"

namespace facebook::cachelib::objcache2 {
template <typename ObjectCache>
class PersistWorker : public PeriodicWorker {
 public:
  struct WorkUnit {
    typename ObjectCache::Key key;
    uintptr_t objectPtr;
    size_t objectSize;
    uint32_t expiryTime;
  };

  using SerializeCb = typename ObjectCache::SerializeCb;
  explicit PersistWorker(uint32_t id,
                         folly::File file,
                         SerializeCb& serializeCb,
                         folly::MPMCQueue<WorkUnit>& queue)
      : id_(id),
        serializeCb_(serializeCb),
        queue_(queue),
        recordWriter_(navy::createFileRecordWriter(std::move(file))) {}

  // Consume the MPMC queue to persist objects.
  void work() override;

  std::string getName() { return folly::sformat("PersistWorker_{}", id_); }

 private:
  uint32_t id_;
  SerializeCb& serializeCb_;
  folly::MPMCQueue<WorkUnit>& queue_;
  std::unique_ptr<RecordWriter> recordWriter_;
};

template <typename ObjectCache>
class Persistor {
 public:
  using PersistWorker = PersistWorker<ObjectCache>;
  using WorkUnit = typename PersistWorker::WorkUnit;
  using SerializeCb = typename PersistWorker::SerializeCb;

  explicit Persistor(uint32_t threadCount,
                     std::string baseFilePath,
                     SerializeCb& serializeCb,
                     ObjectCache& objCache)
      : queue_(folly::MPMCQueue<WorkUnit>(kQueueSize_)), objCache_(objCache) {
    // persist metadata
    try {
      // create a new base file if not exist or write from the beginning
      auto basefile = folly::File(baseFilePath, O_CREAT | O_WRONLY | O_TRUNC);
      auto rw = navy::createFileRecordWriter(std::move(basefile));
      persistence::Metadata metadata;
      metadata.threadCount().value() = threadCount;
      auto iobuf = Serializer::serializeToIOBuf(metadata);
      rw->writeRecord(std::move(iobuf));
    } catch (const std::exception& e) {
      XLOGF(ERR,
            "Persistor initialization failed: Failed to write metadata, reason "
            "= {}",
            folly::exceptionStr(e));
      initSuccess_ = false;
      return;
    }

    // persist objects
    for (uint32_t i = 0; i < threadCount; i++) {
      try {
        // create a new file if not exist or write from the beginning
        auto file = folly::File(getPersistFilePath(baseFilePath, i),
                                O_CREAT | O_WRONLY | O_TRUNC);
        workers_.emplace_back(std::make_unique<PersistWorker>(
            i, std::move(file), serializeCb, queue_));
      } catch (const std::exception& e) {
        XLOGF(ERR,
              "Persistor initialization failed: Failed to create persist "
              "worker {}, reason = {}",
              i, folly::exceptionStr(e));
        initSuccess_ = false;
        break;
      }
    }
  }

  // Start persist workers in different threads; meanwhile add objects to the
  // MPMC queue.
  // @return false if Persistor initialization failed
  bool run();

  // @return number of expired objects that are not persisted.
  uint32_t getNumExpired() { return numExpired_; }

  // @return the file path for ith worker
  static inline std::string getPersistFilePath(std::string& basePath,
                                               uint32_t workerId) {
    return folly::sformat("{}_{}", basePath, workerId);
  }

 private:
  // size of the MPMC Queue
  static constexpr uint32_t kQueueSize_{1000};
  // persistor sleep interval while waiting for all workers to finish
  static constexpr std::chrono::seconds kSleepInterval_{1};
  // persist worker sleep interval
  static constexpr std::chrono::milliseconds kWorkerInterval_{1};

  bool initSuccess_{true};
  folly::MPMCQueue<WorkUnit> queue_;
  std::vector<std::unique_ptr<PersistWorker>> workers_;
  ObjectCache& objCache_;
  uint32_t numExpired_{0};
};

template <typename ObjectCache>
class RestoreWorker {
 public:
  using DeserializeCb = typename ObjectCache::DeserializeCb;

  explicit RestoreWorker(uint32_t id,
                         folly::File file,
                         DeserializeCb& deserializeCb,
                         ObjectCache& objCache)
      : id_(id), deserializeCb_(deserializeCb), objCache_(objCache) {
    recordReader_ = navy::createFileRecordReader(std::move(file));
  }

  // Restore objects to the cache.
  void work();

  // @return number of expired objects in a worker thread
  uint32_t getNumExpired() { return numExpired_; }

  std::string getName() { return folly::sformat("RestoreWorker_{}", id_); }

 private:
  uint32_t id_;
  DeserializeCb& deserializeCb_;
  ObjectCache& objCache_;
  std::unique_ptr<RecordReader> recordReader_;
  uint32_t numExpired_{0};
};

template <typename ObjectCache>
class Restorer {
 public:
  using RestoreWorker = RestoreWorker<ObjectCache>;
  using DeserializeCb = typename RestoreWorker::DeserializeCb;

  explicit Restorer(std::string& baseFilePath,
                    DeserializeCb& deserializeCb,
                    ObjectCache& objCache);

  // Start restore workers in different threads.
  // @return false if Restorer initialization failed
  bool run();

  // @return number of expired objects that are not restored.
  uint32_t getNumExpired() { return numExpired_; }

 private:
  bool initSuccess_{true};
  std::vector<std::unique_ptr<RestoreWorker>> workers_;
  std::atomic<uint32_t> numExpired_{0};
};

template <typename ObjectCache>
void PersistWorker<ObjectCache>::work() {
  WorkUnit workUnit;
  while (queue_.read(workUnit)) {
    // serialize the object
    auto payloadIobuf = serializeCb_(
        typename ObjectCache::Serializer(workUnit.key, workUnit.objectPtr));
    if (!payloadIobuf) {
      XLOG_EVERY_N(ERR, 1000) << folly::sformat(
          "Failed to serialize object for key = {}", workUnit.key);
      continue;
    }

    // serialize persistentItem
    persistence::Item persistentItem;
    persistentItem.key().value() = workUnit.key;
    persistentItem.objectSize().value() = workUnit.objectSize;
    persistentItem.expiryTime().value() = workUnit.expiryTime;
    persistentItem.payload().value().resize(payloadIobuf->length());
    std::memcpy(persistentItem.payload().value().data(), payloadIobuf->data(),
                payloadIobuf->length());
    auto iobuf = Serializer::serializeToIOBuf(persistentItem);
    recordWriter_->writeRecord(std::move(iobuf));
  }
}

template <typename ObjectCache>
bool Persistor<ObjectCache>::run() {
  if (!initSuccess_) {
    return false;
  }
  // start all workers
  for (auto& worker : workers_) {
    worker->start(kWorkerInterval_, worker->getName());
  }

  std::vector<typename ObjectCache::EvictionIterator> evictionItrs;
  auto poolIds = objCache_.l1Cache_->getRegularPoolIds();
  for (auto poolId : poolIds) {
    evictionItrs.emplace_back(objCache_.getEvictionIterator(poolId));
  }

  size_t finished = 0;
  // round-robin each eviction iterator until all iterators are finished
  while (finished < evictionItrs.size()) {
    finished = 0; // reset flag
    for (auto& itr : evictionItrs) {
      if (!itr) { // finished
        finished++;
        continue;
      }
      // no need to persist if item is already expired
      if (itr->isExpired()) {
        numExpired_++;
      } else { // write the object to queue
        auto itemPtr =
            reinterpret_cast<typename ObjectCache::Item*>(itr->getMemory());
        WorkUnit unit{itr->getKey(), itemPtr->objectPtr, itemPtr->objectSize,
                      itr->getExpiryTime()};
        queue_.blockingWrite(std::move(unit));
      }
      ++itr;
    }
  }

  XLOGF(INFO, "Persistor found {} expired objects", numExpired_);

  // Wait until all items in the queue are consumed and persisted
  while (!queue_.isEmpty()) {
    // @lint-ignore facebook-hte-BadCall-sleep_for
    // Sleep is intended here until the persistence is complete
    std::this_thread::sleep_for(kSleepInterval_);
  }

  // stop all workers
  for (auto& worker : workers_) {
    if (!worker->stop()) {
      XLOG(ERR) << folly::sformat("{} failed to stop", worker->getName());
    }
  }
  return true;
}

template <typename ObjectCache>
Restorer<ObjectCache>::Restorer(std::string& baseFilePath,
                                DeserializeCb& deserializeCb,
                                ObjectCache& objCache) {
  // restore metadata
  uint32_t threadCount = 0;
  try {
    auto basefile = folly::File(baseFilePath, O_RDONLY);
    auto rr = navy::createFileRecordReader(std::move(basefile));
    if (!rr->isEnd()) {
      auto iobuf = rr->readRecord();
      Deserializer deserializer(iobuf->data(), iobuf->data() + iobuf->length());
      auto metadata = deserializer.deserialize<persistence::Metadata>();
      threadCount = metadata.threadCount().value();
    }
  } catch (const std::exception& e) {
    XLOGF(ERR,
          "Restorer initialization failed: Failed to read metadata, reason "
          "= {}",
          folly::exceptionStr(e));
    initSuccess_ = false;
    return;
  }

  // restore objects
  for (uint32_t i = 0; i < threadCount; i++) {
    try {
      auto file = folly::File(
          ObjectCache::Persistor::getPersistFilePath(baseFilePath, i),
          O_RDONLY);
      workers_.emplace_back(std::make_unique<RestoreWorker>(
          i, std::move(file), deserializeCb, objCache));
    } catch (const std::exception& e) {
      XLOGF(ERR,
            "Restorer initialization failed: Failed to create restore worker "
            "{}, reason = {}",
            i, folly::exceptionStr(e));
      initSuccess_ = false;
      break;
    }
  }
}

template <typename ObjectCache>
void RestoreWorker<ObjectCache>::work() {
  uint32_t currentTime = util::getCurrentTimeSec();
  while (!recordReader_->isEnd()) {
    auto iobuf = recordReader_->readRecord();
    // deserialize persistentItem
    Deserializer deserializer(iobuf->data(), iobuf->data() + iobuf->length());
    auto persistentItem = deserializer.deserialize<persistence::Item>();
    uint32_t expiryTime = persistentItem.expiryTime().value();
    // no need to recover if object is already expired
    if (expiryTime > 0 && expiryTime <= currentTime) {
      numExpired_++;
      continue;
    }
    // deserialize and insert object
    uint32_t ttlSecs = (expiryTime == 0) ? 0 : expiryTime - currentTime;
    try {
      bool success = deserializeCb_(typename ObjectCache::Deserializer(
          persistentItem.key().value(), persistentItem.payload().value(),
          persistentItem.objectSize().value(), ttlSecs, objCache_));
      if (!success) {
        XLOG_EVERY_N(ERR, 1000)
            << folly::sformat("{} failed to deserialize object for key = {}",
                              getName(), persistentItem.key().value());
      }
    } catch (const std::exception& e) {
      XLOG_EVERY_N(ERR, 1000) << folly::sformat(
          "{} failed to deserialize object for key = {}, exception "
          "= {}",
          getName(),
          persistentItem.key().value(),
          folly::exceptionStr(e));
    }
  }
}

template <typename ObjectCache>
bool Restorer<ObjectCache>::run() {
  if (!initSuccess_) {
    return false;
  }
  std::vector<std::thread> ts;
  for (size_t i = 0; i < workers_.size(); i++) {
    ts.emplace_back(std::thread{[&](int i) {
                                  workers_[i]->work();
                                  // accumulate expired object number
                                  numExpired_ += workers_[i]->getNumExpired();
                                },
                                i});
  }

  // start restoreWorkers in different threads
  for (auto& t : ts) {
    t.join();
  }
  XLOGF(INFO, "Restorer found {} expired objects", numExpired_);
  return true;
}
} // namespace facebook::cachelib::objcache2

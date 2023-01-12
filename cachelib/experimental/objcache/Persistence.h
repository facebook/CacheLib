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

#include <folly/logging/xlog.h>

#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/common/PeriodicWorker.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/experimental/objcache/gen-cpp2/ObjectCachePersistence_types.h"
#include "cachelib/navy/serialization/RecordIO.h"
#include "folly/MPMCQueue.h"

#pragma once

namespace facebook {
namespace cachelib {
namespace objcache {

template <typename SerializationCallback>
class PersistorWorker : public PeriodicWorker {
 public:
  using ItemHandle = CacheAllocator<LruCacheTrait>::WriteHandle;
  using WorkUnit = std::vector<std::pair<ItemHandle, PoolId>>;
  explicit PersistorWorker(SerializationCallback serializationCallback,
                           folly::MPMCQueue<WorkUnit>& queue,
                           std::unique_ptr<RecordWriter> rw)
      : serializationCallback_{std::move(serializationCallback)},
        queue_{queue},
        recordWriter_{std::move(rw)},
        breakOut_{false} {}

  void work() override {
    XDCHECK(serializationCallback_);
    WorkUnit workUnit;
    uint64_t failedToSerializeKeysCount = 0;
    while (!breakOut_.load() && queue_.read(workUnit)) {
      for (auto& [handle, poolId] : workUnit) {
        // no need to persist if item is already expired
        if (handle->isExpired()) {
          continue;
        }

        auto iobuf =
            serializationCallback_(handle->getKey(), handle->getMemory());
        if (!iobuf) {
          failedToSerializeKeysCount++;
          XLOG_EVERY_MS(ERR, 1000)
              << "Failed to serialize for key: " << handle->getKey();
          continue;
        }
        serialization::Item item;
        item.poolId().value() = poolId;
        item.creationTime().value() = handle->getCreationTime();
        item.expiryTime().value() = handle->getExpiryTime();
        item.key().value() = handle->getKey().str();
        item.payload().value().resize(iobuf->length());
        std::memcpy(item.payload().value().data(), iobuf->data(),
                    iobuf->length());
        recordWriter_->writeRecord(Serializer::serializeToIOBuf(item));
      }
    }
    XLOG_EVERY_MS(INFO, 10000)
        << "Failed to Serialize Key Count = " << failedToSerializeKeysCount;
  }

  void setBreakOut() { breakOut_ = true; }

 private:
  SerializationCallback serializationCallback_;
  folly::MPMCQueue<WorkUnit>& queue_;
  std::unique_ptr<RecordWriter> recordWriter_;
  std::atomic<bool> breakOut_;
};

template <typename ObjectCache>
class ObjectCachePersistor {
 public:
  using AccessIterator = CacheAllocator<LruCacheTrait>::AccessIterator;
  using ItemHandle = CacheAllocator<LruCacheTrait>::WriteHandle;
  using WorkUnit = std::vector<std::pair<ItemHandle, PoolId>>;
  using PersistorWorkerObj =
      PersistorWorker<typename ObjectCache::SerializationCallback>;
  explicit ObjectCachePersistor(
      uint32_t numOfThreads,
      typename ObjectCache::SerializationCallback serializationCallback,
      ObjectCache& objcache,
      const std::string& fileName,
      uint32_t queueBatchSize)
      : queue_{folly::MPMCQueue<WorkUnit>(kQueueSize_)},
        objcache_(objcache),
        queuBatchSize_(queueBatchSize) {
    for (size_t i = 0; i < numOfThreads; i++) {
      auto file = folly::File(fileName + "_" + folly::to<std::string>(i),
                              O_WRONLY | O_CREAT | O_TRUNC);
      if (file) {
        auto rw = navy::createFileRecordWriter(std::move(file));
        persistors_.emplace_back(std::make_shared<PersistorWorkerObj>(
            serializationCallback, queue_, std::move(rw)));
        persistors_.back()->start(std::chrono::milliseconds{1});
      }
    }
  }

  ~ObjectCachePersistor() {
    for (auto& persistor : persistors_) {
      persistor->setBreakOut();
      persistor->stop();
    }
  }

  void run() {
    // iterate thorugh cache and insert vector of <key, poolId> with size of
    // queuBatchSize_ into MPMC
    WorkUnit workUnit;
    workUnit.reserve(queuBatchSize_);
    for (auto it = objcache_.getCacheAlloc().begin();
         it != objcache_.getCacheAlloc().end();
         ++it) {
      auto poolId =
          objcache_.getCacheAlloc().getAllocInfo(it->getMemory()).poolId;
      workUnit.emplace_back(std::make_pair<ItemHandle, PoolId>(
          it.asHandle().clone(), std::move(poolId)));
      if (workUnit.size() == queuBatchSize_) {
        while (true) {
          // need to make sure write happens so we wait until queue can accept
          // new items
          if (queue_.writeIfNotFull(std::move(workUnit))) {
            break;
          }
          std::this_thread::yield();
        }
        workUnit.clear();
      }
    }

    // any left over items if last batch is smaller than queuBatchSize_
    if (!workUnit.empty()) {
      while (true) {
        if (queue_.writeIfNotFull(std::move(workUnit))) {
          break;
        }
      }
      workUnit.clear();
    }

    // Wait until all items in MPMC are consumed and persisted
    while (true) {
      if (queue_.isEmpty()) {
        break;
      }
      std::this_thread::yield();
    }

    // stop all workers
    for (auto& persistor : persistors_) {
      persistor->setBreakOut();
      persistor->stop();
    }
  }

 private:
  const static uint32_t kQueueSize_ = 1000;
  folly::MPMCQueue<WorkUnit> queue_;
  std::vector<std::shared_ptr<PersistorWorkerObj>> persistors_;
  ObjectCache& objcache_;
  uint32_t queuBatchSize_;
};

template <typename ObjectCache>
class RestorerWorker : public PeriodicWorker {
 public:
  using DeserializationCallback = typename ObjectCache::DeserializationCallback;
  explicit RestorerWorker(DeserializationCallback deserializationCallback,
                          ObjectCache& objcache,
                          std::shared_ptr<RecordReader> rr,
                          size_t threadNum)
      : deserializationCallback_{std::move(deserializationCallback)},
        objcache_(objcache),
        recordReader_(std::move(rr)),
        breakOut_(false),
        name_{folly::sformat("thread_{}", threadNum)} {}

  void work() override {
    XDCHECK(deserializationCallback_);
    uint32_t currentTime = static_cast<uint32_t>(util::getCurrentTimeSec());
    while (!breakOut_.load() && !recordReader_->isEnd()) {
      auto iobuf = recordReader_->readRecord();
      XDCHECK(iobuf);
      Deserializer deserializer(iobuf->data(), iobuf->data() + iobuf->length());
      auto item = deserializer.deserialize<serialization::Item>();
      // no need to restore if item is already expired
      if (isExpired(item.expiryTime().value(), currentTime)) {
        continue;
      }
      auto hdl = deserializationCallback_(
          item.poolId().value(), item.key().value(), item.payload().value(),
          item.creationTime().value(), item.expiryTime().value(), objcache_);
      if (!hdl) {
        XLOG_EVERY_MS(ERR, 1000)
            << "Failed to deserialize for key: " << item.key().value();
        continue;
      }
      try {
        objcache_.getCacheAlloc().insertOrReplace(hdl);
      } catch (const std::exception& e) {
        XLOG_EVERY_N(INFO, 1000)
            << "Failed to insert item, reason = " << folly::exceptionStr(e);
        continue;
      }
    }
  }

  void setBreakOut() { breakOut_ = true; }

  bool isExpired(uint32_t expiryTime, uint32_t nowTime) {
    if (expiryTime != 0 && expiryTime < nowTime) {
      return true;
    }

    return false;
  }

  const std::string& getName() const { return name_; }

 private:
  DeserializationCallback deserializationCallback_;
  ObjectCache& objcache_;
  std::shared_ptr<RecordReader> recordReader_;
  std::atomic<bool> breakOut_;
  std::string name_;
};

template <typename ObjectCache>
class ObjectCacheRestorer {
 public:
  using DeserializationCallback = typename ObjectCache::DeserializationCallback;
  using RestorerWorkerObj = RestorerWorker<ObjectCache>;
  explicit ObjectCacheRestorer(uint32_t numOfThreads,
                               DeserializationCallback deserializationCallback,
                               ObjectCache& objcache,
                               const std::string& fileName,
                               uint32_t timeOutDurationInSec)
      : objcache_(objcache), timeOutDurationInSec_(timeOutDurationInSec) {
    for (size_t i = 0; i < numOfThreads; i++) {
      auto file = folly::File(fileName + "_" + folly::to<std::string>(i));
      if (file) {
        recordReaders_.emplace_back(
            navy::createFileRecordReader(std::move(file)));
        restorers_.emplace_back(std::make_shared<RestorerWorkerObj>(
            deserializationCallback, objcache, recordReaders_.at(i), i));
        restorers_.back()->start(std::chrono::milliseconds{1});
      }
    }
  }

  ~ObjectCacheRestorer() {
    for (auto& restorer : restorers_) {
      if (!restorer->stop()) {
        XLOG(ERR)
            << "Destruction: Restorer thread did not stop, thread name =  "
            << restorer->getName();
      }
    }
  }

  void run() {
    auto recoveryStartTime = std::chrono::system_clock::now();
    uint32_t timeElapsedInSec = 0;
    bool forceStopWorker = false;
    while (true) {
      bool allReadersAreEmpty = true;
      for (const auto& reader : recordReaders_) {
        allReadersAreEmpty = allReadersAreEmpty && reader->isEnd();
      }

      if (allReadersAreEmpty) {
        break;
      }

      timeElapsedInSec =
          timeOutDurationInSec_ > 0
              ? std::chrono::duration_cast<std::chrono::seconds>(
                    std::chrono::system_clock::now() - recoveryStartTime)
                    .count()
              : 0;
      if (timeElapsedInSec > timeOutDurationInSec_) {
        XLOG(INFO) << "Recover timed out and couldn't finish completely, "
                      "timeOutDurationInSec =  "
                   << timeOutDurationInSec_
                   << ", timeElapsedInSec = " << timeElapsedInSec;

        forceStopWorker = true;
        break;
      }

      // sleep override
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    for (auto& restorer : restorers_) {
      if (forceStopWorker) {
        restorer->setBreakOut();
      }
      if (!restorer->stop()) {
        XLOG(ERR)
            << "forceStopWorker: Restorer thread did not stop, thread name =  "
            << restorer->getName();
        ;
      }
    }
  }

 private:
  std::vector<std::shared_ptr<RestorerWorkerObj>> restorers_;
  std::vector<std::shared_ptr<RecordReader>> recordReaders_;
  ObjectCache& objcache_;
  uint32_t timeOutDurationInSec_;
};

} // namespace objcache
} // namespace cachelib
} // namespace facebook

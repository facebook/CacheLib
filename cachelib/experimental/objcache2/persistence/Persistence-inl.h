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

namespace facebook {
namespace cachelib {
namespace objcache2 {

template <typename ObjectCache>
void PersistWorker<ObjectCache>::work() {
  WorkUnit workUnit;
  while (queue_.read(workUnit)) {
    // serialize the object
    auto payloadIobuf = serializeCb_(
        typename ObjectCache::Serializer(workUnit.key, workUnit.objectPtr));
    if (!payloadIobuf) {
      XLOG_EVERY_N(INFO, 1000) << folly::sformat(
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
        queue_.write(std::move(unit));
      }
      ++itr;
    }
  }

  XLOGF(INFO, "Persistor found {} expired objects", numExpired_);

  // Wait until all items in the queue are consumed and persisted
  while (!queue_.isEmpty()) {
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
        XLOG_EVERY_N(INFO, 1000)
            << folly::sformat("{} failed to deserialize object for key = {}",
                              getName(), persistentItem.key().value());
      }
    } catch (const std::exception& e) {
      XLOG_EVERY_N(INFO, 1000) << folly::sformat(
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

} // namespace objcache2
} // namespace cachelib
} // namespace facebook

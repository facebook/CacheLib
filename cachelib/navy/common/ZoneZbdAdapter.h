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

#include <string>
#include <map>
#include <thread>
#include <queue>
#include <mutex>
#include <atomic>
#include <fcntl.h>
#include <libaio.h>
#include <folly/File.h>
#include <folly/SharedMutex.h>
#include <libzbd/zbd.h>

using namespace std;
namespace facebook {
namespace cachelib {
namespace navy {
const uint64_t INDEX_PAREL_NUM = 128 * 1024;
const uint64_t ZONE_NUM_IN_NODE = 16;
#define CTX_NUM 1024

struct zone {
  uint64_t         offset;
  uint64_t         wp;
  atomic<uint64_t> valid;
  uint8_t          lock;
  uint8_t          state;
};

struct VZone {
  uint32_t         zones[ZONE_NUM_IN_NODE];
  uint32_t         usedSize;
  atomic<uint64_t> validSize;
  uint8_t          status;
};

struct RegionMeta {
  int16_t  nodeId;
  uint32_t offset;
};

struct WriteUnit {
  void*    buf;
  uint64_t zoneOffset;
  uint32_t zoneId;
  uint32_t size;
};

struct ReadUnit {
  void*    buf;
  uint64_t zoneOffset;
  uint32_t size;
};

class ZoneZbdAdapter {
 public:
  ZoneZbdAdapter() {
    hashStart_    = 0;
    blockStart_   = 0;
    readWriteF_   = -1;
    zones_        = nullptr;
    mutex_        = nullptr;
    logicToPhyArr = nullptr;
    ctx_          = nullptr;
    nodes_        = nullptr;
    regionFTLArr  = nullptr;
    zoneNum_      = 0;
    zoneCapacity_ = 0;
    blockSize_    = 0;
    hashZoneIndex_= 0;
    isRunning     = false;
    resetThread   = nullptr;
  }

  virtual ~ZoneZbdAdapter();

  bool openZoneDevice(const string& devName);

  bool readOffset(void* buf, uint64_t offset, uint64_t size);

  bool writeOffset(const void* buf, uint64_t offset, uint64_t size);

  void setLayOutInfo(uint64_t blockCacheStart, uint64_t blockCacheSize, uint64_t hashStart,
                     uint64_t bucketNum);

  int resetZone(uint64_t offset, uint64_t len);

  int resetRegion(uint64_t offset, uint64_t len);

 private:
  void praseZoneInfo(struct zbd_zone* rep);

  int allocZone(uint64_t size);

  void repaidZone(uint32_t zoneId, bool free = false);

  int allocNode(uint64_t size);

  void repaidNode(int16_t nodeId);

  bool readSync(void* buf, uint64_t offset, uint64_t size);

  bool writeSync(const void* buf, uint64_t offset, uint64_t size);

  void processReset();

  void getWriteEvents(int* lock);

  bool writeAsync(const void* buf, uint64_t offset, uint64_t size);

  void splitRegionRead(void* buf, uint64_t offset, uint64_t size,
                       vector<struct ReadUnit>& rus);

  bool readRegion(void* buf, uint64_t offset, uint64_t size);

  folly::SharedMutex& getMutex(uint32_t zid) const {
    return mutex_[zid & (INDEX_PAREL_NUM - 1)];
  }

  int          readWriteF_;
  struct zone* zones_;

  queue<uint32_t> freeQue;
  queue<uint32_t> usedQue;
  std::mutex      allocMutex;

  struct VZone*      nodes_;
  queue<uint16_t>    freeNodeQue;
  queue<uint16_t>    usedNodeQue;
  std::mutex         allocNodeMutex;
  struct RegionMeta* regionFTLArr;

  uint32_t zoneNum_;
  uint64_t zoneCapacity_;
  uint32_t blockSize_;
  uint32_t hashZoneIndex_;
  uint64_t hashStart_;
  uint64_t blockStart_;

  uint64_t*           logicToPhyArr;
  folly::SharedMutex* mutex_;

  std::thread*    resetThread;
  queue<uint32_t> resetQue;
  std::mutex      resetMutex;
  bool            isRunning;

  io_context_t    ctx_;
  struct io_event events[CTX_NUM];
};

}  // namespace navy
}  // namespace cachelib
}  // namespace facebook

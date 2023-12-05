
#include <stdio.h>
#include <cstring>
#include <numeric>
#include <iostream>
#include "ZoneZbdAdapter.h"
#include "cachelib/common/Time.h"

namespace facebook {
namespace cachelib {
namespace navy {
#define INVALID       0xFFFFFFFFFFFFFFFF
#define INVALID_NODE  -1
#define OPTIMAL_WRITE (16 * 4096)
const int32_t REGION_ONE_WRITE = OPTIMAL_WRITE * ZONE_NUM_IN_NODE;
struct WriteIoCB {
  struct iocb cb;
  int         index;
  int*        complete;
};

struct ReadSeg {
  void*    buf;
  uint64_t offset;
  uint64_t size;
};

ZoneZbdAdapter::~ZoneZbdAdapter() {
  for (int i = 0; i < zoneNum_; i++) {
    if (zones_[i].valid > 0 && zones_[i].wp != zones_[i].offset) {
      uint64_t valid = zones_[i].valid;
      cout << "~ZoneZbdAdapter " << i
           << " all: " << zones_[i].wp - zones_[i].offset
           << " valid: " << zones_[i].valid << " valid: "
           << (zones_[i].wp - zones_[i].offset) -
                  (zoneCapacity_ - zones_[i].valid)
           << endl;
    }
  }

  isRunning = false;
  resetThread->join();
  delete resetThread;
  delete[] zones_;
  if (mutex_) {
    delete[] mutex_;
  }
  if (logicToPhyArr) {
    delete[] logicToPhyArr;
  }

  if (nodes_) {
    delete[] nodes_;
  }

  if (regionFTLArr) {
    delete[] regionFTLArr;
  }

  int ret = io_destroy(ctx_);
  if (ret < 0) {
    printf("io_destroy: ret[%d] \n", ret);
  }
  zbd_close(readWriteF_);
}

bool ZoneZbdAdapter::openZoneDevice(const string& devName) {
  struct zbd_info info;
  readWriteF_ = zbd_open(devName.c_str(), O_RDWR | O_DIRECT, &info);
  if (readWriteF_ < 0) {
    printf("zbd_open: err readWriteF_[%d] \n", readWriteF_);
    return false;
  }

  zoneNum_      = info.nr_zones;
  zoneCapacity_ = info.zone_size;
  blockSize_    = info.pblock_size;

  uint64_t         addrSpaceSize = zoneNum_ * zoneCapacity_;
  struct zbd_zone* zoneRep       = NULL;
  uint32_t         reportZoneNum = 0;
  int ret = zbd_list_zones(readWriteF_, 0, addrSpaceSize, ZBD_RO_ALL, &zoneRep,
                           &reportZoneNum);
  if (ret || reportZoneNum != zoneNum_) {
    printf("zbd_list_zones : ret[%d] reportZoneNum[%u] != zoneNum_[%u] \n", ret,
           reportZoneNum, zoneNum_);
    return false;
  }

  zones_ = new zone[zoneNum_];
  praseZoneInfo(zoneRep);
  resetThread = new std::thread(&ZoneZbdAdapter::processReset, this);
  isRunning   = true;

  ret = io_setup(CTX_NUM, &ctx_);
  if (ret) {
    printf("writeAsync: io_setup failed ret[%d] \n", ret);
  }

  memset(events, 0, sizeof(events));
  free(zoneRep);

  return true;
}

void ZoneZbdAdapter::setLayOutInfo(uint64_t blockCacheStart, uint64_t blockCacheSize, 
    uint64_t hashStart, uint64_t bucketNum) {
  hashStart_     = hashStart;
  blockStart_    = blockCacheStart;
  hashZoneIndex_ = hashStart_ % zoneCapacity_ ? hashStart_ / zoneCapacity_ + 1
                                              : hashStart_ / zoneCapacity_;
  printf("ZoneZbdAdapter::setLayOutInfo blockCacheStart [%lu] blockCacheSize[%lu] hashStart_[%lu] "
      "hashZoneIndex_[%lu] bucketNum[%llu]\n",
      blockCacheStart, blockCacheSize, hashStart_, hashZoneIndex_, bucketNum);
  for (int free = hashZoneIndex_; free < zoneNum_; free++) {
    freeQue.push(free);
  }

  int num       = zoneNum_ - hashZoneIndex_;
  mutex_        = new folly::SharedMutex[INDEX_PAREL_NUM];
  logicToPhyArr = new uint64_t[bucketNum];
  for (int i = 0; i < bucketNum; i++) {
    logicToPhyArr[i] = INVALID;
  }

  uint64_t vzoneNum = blockCacheSize / ZONE_NUM_IN_NODE / zoneCapacity_ + 1;
  nodes_            = new VZone[vzoneNum];
  int nodeId        = 0;
  for (uint32_t zoneId = 1; zoneId < hashZoneIndex_;) {
    if (hashZoneIndex_ - zoneId < ZONE_NUM_IN_NODE) {
      break;
    }

    struct VZone* vzone = &nodes_[nodeId];
    vzone->usedSize     = 0;
    vzone->validSize    = 0;
    for (int tz = 0; tz < ZONE_NUM_IN_NODE; tz++) {
      vzone->zones[tz] = zoneId++;
    }
    freeNodeQue.push(nodeId++);
  }

  uint64_t regionMetaNum = blockCacheSize / REGION_ONE_WRITE + 1;
  regionFTLArr           = new RegionMeta[regionMetaNum];
  for (uint64_t i = 0; i < regionMetaNum; i++) {
    regionFTLArr[i].nodeId = INVALID_NODE;
  }
}

bool ZoneZbdAdapter::readOffset(void* buf, uint64_t offset, uint64_t size) {
  if (offset < hashStart_) {
    return readRegion(buf, offset, size);
  } else {
    int      index     = (offset - hashStart_) / size;
    uint64_t targetOff = offset;
    {
      std::unique_lock<folly::SharedMutex> lock{getMutex(index)};
      targetOff = logicToPhyArr[index];
    }

    if (targetOff == INVALID) {
      targetOff = offset;
    }

    return readSync(buf, targetOff, size);
  }
}

bool ZoneZbdAdapter::writeOffset(const void* buf, uint64_t offset,
                                 uint64_t size) {
  if (offset < hashStart_) {
    return writeAsync(buf, offset, size);
  } else {
    int      index     = (offset - hashStart_) / size;
    uint64_t targetOff = offset;
    {
      std::unique_lock<folly::SharedMutex> lock{getMutex(index)};
      targetOff = logicToPhyArr[index];
    }

    if (targetOff != INVALID) {
      int zindex = targetOff / zoneCapacity_;
      zones_[zindex].valid -= size;
      if (zones_[zindex].valid == 0) {
        resetMutex.lock();
        resetQue.push(zindex);
        resetMutex.unlock();
      }
    }

    int zoneIndex = allocZone(size);
    if (zoneIndex >= zoneNum_) {
      printf("no zone!");
      return false;
    }

    bool ret = writeSync(buf, zones_[zoneIndex].wp, size);
    if (ret) {
      std::unique_lock<folly::SharedMutex> lock{getMutex(index)};
      logicToPhyArr[index] = zones_[zoneIndex].wp;
      zones_[zoneIndex].wp += size;
    }

    repaidZone(zoneIndex);
    zones_[zoneIndex].lock = 0;
    return ret;
  }
}

void ZoneZbdAdapter::praseZoneInfo(struct zbd_zone* rep) {
  for (int i = 0; i < zoneNum_; i++) {
    zone*            z     = &zones_[i];
    struct zbd_zone* zInfo = &rep[i];
    z->offset              = zInfo->start;
    z->wp                  = zInfo->wp;
    z->lock                = 0;
    z->state               = zInfo->flags;
    z->valid               = zoneCapacity_;
  }
}

int ZoneZbdAdapter::allocZone(uint64_t size) {
  const int ONCE_ALLOC = 5;
  allocMutex.lock();
  if (usedQue.empty()) {
    int allocNum = 0;
    while (!freeQue.empty() && allocNum < ONCE_ALLOC) {
      usedQue.push(freeQue.front());
      freeQue.pop();
      allocNum++;
    }
  }

  int allockId = zoneNum_;  // may dead circle
  while (!usedQue.empty()) {
    int i = usedQue.front();
    usedQue.pop();
    if (zones_[i].wp + size > zones_[i].offset + zoneCapacity_) {
      usedQue.push(i);
      continue;
    }

    allockId = i;
    break;
  }

  if (allockId == zoneNum_) {
    printf("no zoned avaliable!\n");
  }
  allocMutex.unlock();
  return allockId;
}

void ZoneZbdAdapter::repaidZone(uint32_t zoneId, bool free) {
  if (zones_[zoneId].wp < zones_[zoneId].offset + zoneCapacity_) {
    allocMutex.lock();
    if (free) {
      freeQue.push(zoneId);
    } else {
      usedQue.push(zoneId);
    }
    allocMutex.unlock();
  }
}
int ZoneZbdAdapter::allocNode(uint64_t size) {
  const int ONCE_ALLOC = 5;
  allocNodeMutex.lock();
  if (usedNodeQue.empty()) {
    int allocNum = 0;
    while (!freeNodeQue.empty() && allocNum < ONCE_ALLOC) {
      usedNodeQue.push(freeNodeQue.front());
      freeNodeQue.pop();
      allocNum++;
    }
  }

  int allockId = -1;  // may dead circle
  while (!usedNodeQue.empty()) {
    int i = usedNodeQue.front();
    usedNodeQue.pop();
    if (nodes_[i].usedSize + size > ZONE_NUM_IN_NODE * zoneCapacity_) {
      usedNodeQue.push(i);
      continue;
    }

    allockId = i;
    break;
  }

  if (allockId == -1) {
    printf("no zoned avaliable!\n");
  }
  allocNodeMutex.unlock();
  return allockId;
}

void ZoneZbdAdapter::repaidNode(int16_t nodeId) {
  if (nodes_[nodeId].usedSize < ZONE_NUM_IN_NODE * zoneCapacity_) {
    allocNodeMutex.lock();
    usedNodeQue.push(nodeId);
    allocNodeMutex.unlock();
  }
}

bool ZoneZbdAdapter::readSync(void* buf, uint64_t offset, uint64_t size) {
  if (offset % blockSize_ != 0 || size % blockSize_ != 0) {
    printf("readSync:no allign buf[%p] offset[%lu] size[%lu]\n", buf, offset,
           size);
    return false;
  }

  int readSize = pread(readWriteF_, buf, size, offset);
  if (readSize <= 0) {
    printf("pread:err readSize[%d] buf[%p] offset[%lu] size[%lu]\n", readSize,
           buf, offset, size);
    return false;
  }

  return readSize == size;
}

bool ZoneZbdAdapter::writeSync(const void* buf, uint64_t offset,
                               uint64_t size) {
  if (offset % blockSize_ != 0 || size % blockSize_ != 0) {
    printf("writeSync:no allign buf[%p] offset[%lu] size[%lu]\n", buf, offset,
           size);
    return false;
  }

  int ret = pwrite(readWriteF_, buf, size, offset);
  if (ret < 0) {
    printf("pwrite: err ret[%d] opaque [%p] offset[%lu] size[%lu]\n", ret, buf,
           offset, size);
    return false;
  }
  return size == ret;
}

void ZoneZbdAdapter::getWriteEvents(int* lock) {
  struct timespec tms;
  tms.tv_sec  = 0;
  tms.tv_nsec = 100000000;
  int r       = io_getevents(ctx_, 1, CTX_NUM, events, &tms);
  if (r > 0) {
    for (int j = 0; j < r; ++j) {
      struct WriteIoCB* iocb = (struct WriteIoCB*)events[j].obj;
      lock[iocb->index%ZONE_NUM_IN_NODE]      = 0;
      int zoneId             = iocb->cb.u.c.offset / zoneCapacity_;
      zones_[zoneId].wp += OPTIMAL_WRITE;
      (*iocb->complete)++;
    }
  }
}

bool ZoneZbdAdapter::writeAsync(const void* buf, uint64_t offset,
                                uint64_t size) {
  if (offset % blockSize_ != 0 || size % blockSize_ != 0) {
    printf("writeAsync:no allign buf[%p] offset[%lu] size[%lu]\n", buf, offset, size);
    return false;
  }

  if ((size / OPTIMAL_WRITE != ZONE_NUM_IN_NODE) ||
      (size % OPTIMAL_WRITE != 0)) {
    printf("writeAsync: no strip buf[%p] offset[%lu] size[%lu] \n", buf, offset, size);
    return false;
  }

  int16_t nodeId = allocNode(size);
  if (nodeId == -1) {
    printf("writeAsync:no nodeId buf[%p] offset[%lu] size[%lu] nodeId [%d]\n",
           buf, offset, size, nodeId);
    return false;
  }

  uint64_t startIndex =
      nodes_[nodeId].usedSize / OPTIMAL_WRITE % ZONE_NUM_IN_NODE;
  uint16_t num = size / OPTIMAL_WRITE;

  vector<vector<struct WriteUnit>> writeUnites(ZONE_NUM_IN_NODE, vector<struct WriteUnit>());
  for (int i = 0; i < num; i++) {
    int                       j    = i % ZONE_NUM_IN_NODE;
    vector<struct WriteUnit>& temp = writeUnites[j];
    struct WriteUnit          wu;
    wu.buf        = (void*)buf + i * OPTIMAL_WRITE;
    wu.size       = OPTIMAL_WRITE;
    wu.zoneId     = nodes_[nodeId].zones[(startIndex + i) % ZONE_NUM_IN_NODE];
    wu.zoneOffset = temp.size() == 0 ? zones_[wu.zoneId].wp : temp[temp.size() - 1].zoneOffset + OPTIMAL_WRITE;
    temp.push_back(wu);
  }

  struct WriteIoCB cbs[ZONE_NUM_IN_NODE];
  memset(cbs, 0, sizeof(cbs));
  int submit                        = 0;
  int lock[ZONE_NUM_IN_NODE]        = {0};
  int submitIndex[ZONE_NUM_IN_NODE] = {0};
  int complete                      = 0;
  for (int i = 0; i < ZONE_NUM_IN_NODE; i++) {
    cbs[i].complete = &complete;
  }

  while (submit < num) {
    for (int i = 0; i < num; i++) {
      int j = i % ZONE_NUM_IN_NODE;
      if (lock[j]) {
        getWriteEvents(lock);
        continue;
      }

      if (submitIndex[j] >= writeUnites[j].size()) {
        continue;
      }

      struct WriteUnit& wu = writeUnites[j][submitIndex[j]];
      io_prep_pwrite(&cbs[j].cb, readWriteF_, wu.buf, wu.size, wu.zoneOffset);
      struct iocb* iocbs[1];
      iocbs[0] = &cbs[j].cb;
      int ret  = io_submit(ctx_, 1, iocbs);
      if (ret != 1) {
        getWriteEvents(lock);
        printf("writeAsync: io_submit failed ret[%d] \n", ret);
      } else {
        submitIndex[j]++;
        submit++;
        lock[j] = 1;
      }
    }
  }

  while (complete < num) {
    getWriteEvents(lock);
    usleep(1);
  }

  RegionMeta rm;
  rm.nodeId = nodeId;
  rm.offset = nodes_[nodeId].usedSize;
  nodes_[nodeId].usedSize += size;
  nodes_[nodeId].validSize += size;
  regionFTLArr[(offset - blockStart_) / size] = rm;
  repaidNode(nodeId);

  return true;
}

void ZoneZbdAdapter::splitRegionRead(void* buf, uint64_t offset, uint64_t size,
                                     vector<struct ReadUnit>& rus) {
  if (offset % blockSize_ != 0 || size % blockSize_ != 0) {
    printf("readSync:no allign buf[%p] offset[%lu] size[%lu]\n", buf, offset, size);
    return;
  }

  struct ReadSeg rseg;
  rseg.buf    = buf;
  rseg.offset = offset;
  rseg.size   = size;

  queue<ReadSeg> rsegQue;
  rsegQue.push(rseg);
  while (!rsegQue.empty()) {
    rseg = rsegQue.front();
    rsegQue.pop();
    uint32_t blockOff = rseg.offset % (REGION_ONE_WRITE);
    if (blockOff + rseg.size > REGION_ONE_WRITE) {
      uint64_t       rsize = REGION_ONE_WRITE - blockOff;
      struct ReadSeg trseg;
      trseg.buf    = rseg.buf + rsize;
      trseg.offset = rseg.offset + rsize;
      trseg.size   = rseg.size - rsize;
      rsegQue.push(trseg);
      rseg.size = rsize;
    }

    int blockIndex = (rseg.offset - blockStart_) / REGION_ONE_WRITE;
    if (regionFTLArr[blockIndex].nodeId == -1) {
      printf("splitRegionRead: no meta tid[%lu] buf[%p] offset[%lu] size[%lu]\n",
          std::this_thread::get_id(), rseg.buf, rseg.offset, rseg.size);
      continue;
    }

    uint64_t nodeOffset = regionFTLArr[blockIndex].offset;
    uint16_t nodeId     = regionFTLArr[blockIndex].nodeId;
    uint64_t readOffset = nodeOffset + blockOff;
    uint16_t level      = readOffset / REGION_ONE_WRITE;
    uint32_t zoneIndex  = readOffset / OPTIMAL_WRITE % ZONE_NUM_IN_NODE;
    uint64_t zoneOff    = zones_[nodes_[nodeId].zones[zoneIndex]].offset +
                       OPTIMAL_WRITE * level + readOffset % OPTIMAL_WRITE;
    uint64_t singleZoneReadLen =
        rseg.size < (OPTIMAL_WRITE - readOffset % OPTIMAL_WRITE)
            ? rseg.size : (OPTIMAL_WRITE - readOffset % OPTIMAL_WRITE);
    uint64_t remainLen = rseg.size;
    void*    tmpBuf    = rseg.buf;
    while (remainLen > 0) {
      struct ReadUnit ru;
      ru.buf        = tmpBuf;
      ru.zoneOffset = zoneOff;
      ru.size       = singleZoneReadLen;
      rus.push_back(ru);
      remainLen -= singleZoneReadLen;
      tmpBuf += singleZoneReadLen;
      zoneIndex = (zoneIndex + 1) % ZONE_NUM_IN_NODE;
      if (zoneIndex == 0) {
        level++;
      }

      zoneOff = zones_[nodes_[nodeId].zones[zoneIndex]].offset +
                OPTIMAL_WRITE * level;
      singleZoneReadLen = OPTIMAL_WRITE > remainLen ? remainLen : OPTIMAL_WRITE;
    }
  }
}

bool ZoneZbdAdapter::readRegion(void* buf, uint64_t offset, uint64_t size) {
  vector<struct ReadUnit> rus;
  splitRegionRead(buf, offset, size, rus);
  int readComplete = 0;
  for (int i = 0; i < rus.size(); i++) {
    int readSize =
        pread(readWriteF_, rus[i].buf, rus[i].size, rus[i].zoneOffset);
    if (readSize <= 0) {
      return false;
    }
    readComplete += readSize;
  }

  return readComplete == size;
}

int ZoneZbdAdapter::resetRegion(uint64_t offset, uint64_t len) {
 // printf("resetRegion: offset[%lu] len[%lu]\n", offset, len);
  uint64_t toffset = offset - blockStart_;
  for (uint64_t temp = toffset; temp < toffset + len; temp += REGION_ONE_WRITE) {
    RegionMeta* rm = &regionFTLArr[temp / REGION_ONE_WRITE];
    if (rm->nodeId == INVALID_NODE) {
      continue;
    }

    nodes_[rm->nodeId].validSize -= REGION_ONE_WRITE;
    if (nodes_[rm->nodeId].validSize == 0 &&
        nodes_[rm->nodeId].usedSize == ZONE_NUM_IN_NODE * zoneCapacity_) {
      for (int i = 0; i < ZONE_NUM_IN_NODE; i++) {
        resetZone(zones_[nodes_[rm->nodeId].zones[i]].offset, zoneCapacity_);
      }
      repaidNode(rm->nodeId);
    }
    rm->nodeId = INVALID_NODE;
  }

  return 0;
}

int ZoneZbdAdapter::resetZone(uint64_t offset, uint64_t len) {
  // printf("resetZone: offset[%lu] len[%lu]\n",  offset, len);
  if (len != 0) {
    int index = offset / zoneCapacity_;
    int max   = index + len / zoneCapacity_;
    for (; index < max; index++) {
      int ret = zbd_reset_zones(readWriteF_, zones_[index].offset, zoneCapacity_);
      if (ret) {
        printf("resetZone: failed ret[%d] offset[%lu] len[%lu]\n", ret, offset, len);
      }

      zones_[index].wp    = zones_[index].offset;
      zones_[index].lock  = 0;
      zones_[index].valid = zoneCapacity_;
      if (index >= hashZoneIndex_) {
        repaidZone(index, true);
      }
    }
  } else {
    printf("reset BigHash [%d]\n", offset);
    int index = offset % zoneCapacity_ ? offset / zoneCapacity_ + 1
                                       : offset / zoneCapacity_;
    for (; index < zoneNum_; index++) {
      if (zones_[index].wp != zones_[index].offset) {
        int ret =
            zbd_reset_zones(readWriteF_, zones_[index].offset, zoneCapacity_);
        if (ret) {
          printf("zbd reset ret[%d] offset[%lu] len[%lu]\n", ret, zones_[index].offset, zoneCapacity_);
        }
        printf("reset index[%d]\n", index);
        zones_[index].wp    = zones_[index].offset;
        zones_[index].lock  = 0;
        zones_[index].valid = zoneCapacity_;
      }
    }
  }

  return 0;
}

void ZoneZbdAdapter::processReset() {
  while (isRunning) {
    usleep(1);
    resetMutex.lock();
    if (resetQue.empty()) {
      resetMutex.unlock();
      continue;
    }

    int i = resetQue.front();
    resetQue.pop();
    resetMutex.unlock();
    resetZone(zones_[i].offset, zoneCapacity_);
  }
}

}  // namespace navy
}  // namespace cachelib
}  // namespace facebook

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
#include <folly/io/IOBuf.h>

#include "cachelib/allocator/nvmcache/NavyConfig.h"
#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/PercentileStats.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {

class DeviceEncryptor {
 public:
  virtual ~DeviceEncryptor() = default;

  // @return block size of which encryption or decryption payloads must
  //         be aligned with. Otherwise operations will always fail.
  virtual uint32_t encryptionBlockSize() const = 0;

  // @param value   will be overwritten with encrypted value; value must
  //                be aligned to the encryption block size; value must
  //                must be the same in size before and after encryption
  // @param salt    this must be the same salt used later for decryption
  // @return        true if success, false otherwise
  virtual bool encrypt(folly::MutableByteRange value, uint64_t salt) = 0;

  // @param value   will be overwritten with decrypted value; value must
  //                be aligned to the decryption block size; value must
  //                must be the same in size before and after decryption
  // @param salt    this must be the same earlier used for encryption
  // @return        true if success, false otherwise
  virtual bool decrypt(folly::MutableByteRange value, uint64_t salt) = 0;
};

// Device abstraction
//
// Read/write returns true if @value written/read entirely (all @size bytes).
// Pointer ownership is not passed.
//
// For the device supporting the data placement technology like FDP,
// the write API takes the optional argument placeHandle which is used
// to pass the hint for the placement of the data in the device.
// E.g., in case of NVMe FDP, the SSD places the data to a different
// erase unit (a.k.a. superblock) inside the SSD depending on the handle.
// The expectation is that, if the host writes the data in sequential
// overwrite manner (like the BlockCache with FIFO replacement),
// the overwrites makes the whole superblock being invalidated inside
// the SSD. As such, the SSD can reclaim the superblock simply by erasing
// it without needs to copy any valid data (so called garbage collection)
// to a different superblock.
// (https://nvmexpress.org/nvmeflexible-data-placement-fdp-blog/)
class Device {
 public:
  // @param size    total size of the device
  explicit Device(uint64_t size)
      : Device{size, nullptr /* encryptor */, 0 /* max device write size */} {}

  // @param size          total size of the device
  // @param encryptor     encryption object
  // @param maxWriteSize  max device write size
  Device(uint64_t size,
         std::shared_ptr<DeviceEncryptor> encryptor,
         uint32_t maxWriteSize)
      : Device(size,
               std::move(encryptor),
               kDefaultAlignmentSize,
               0 /* max device IO size */,
               maxWriteSize) {}

  // @param size          total size of the device
  // @param encryptor     encryption object
  // @param ioAlignSize   alignment size for IO operations
  // @param maxIOSize     max device IO size
  // @param maxWriteSize  max device write size
  Device(uint64_t size,
         std::shared_ptr<DeviceEncryptor> encryptor,
         uint32_t ioAlignSize,
         uint32_t maxIOSize,
         uint32_t maxWriteSize)
      : size_(size),
        ioAlignmentSize_{ioAlignSize},
        maxIOSize_(maxIOSize),
        maxWriteSize_(maxWriteSize),
        encryptor_{std::move(encryptor)} {
    if (ioAlignSize == 0) {
      throw std::invalid_argument(
          folly::sformat("Invalid ioAlignSize {}", ioAlignSize, size));
    }
    if (encryptor_ && encryptor_->encryptionBlockSize() != ioAlignSize) {
      throw std::invalid_argument(
          folly::sformat("Invalid ioAlignSize {} encryption block size {}",
                         ioAlignSize, encryptor_->encryptionBlockSize()));
    }
    if (maxWriteSize_ % ioAlignmentSize_ != 0) {
      throw std::invalid_argument(folly::sformat(
          "Invalid max write size {} ioAlignSize {}", maxWriteSize_, size));
    }
    if (maxIOSize_ % ioAlignmentSize_ != 0) {
      throw std::invalid_argument(
          folly::sformat("Invalid max io size {} ioAlignSize {}", maxIOSize_,
                         ioAlignmentSize_));
    }
  }
  virtual ~Device() = default;

  // Get the post-alignment size for the size of the data we intend to write
  size_t getIOAlignedSize(size_t size) const {
    return powTwoAlign(size, ioAlignmentSize_);
  }

  // Create an IO buffer of at least @size bytes that can be used for read and
  // write. For example, direct IO device allocates a properly aligned buffer.
  Buffer makeIOBuffer(size_t size) const {
    return Buffer{getIOAlignedSize(size), ioAlignmentSize_};
  }

  // Write buffer to the device. This call takes ownership of the buffer
  // and de-allocates it by end of the call.
  // @param buffer    Data to write to the device. It must be aligned the same
  //                  way as `makeIOBuffer` would return.
  // @param offset    Must be ioAlignmentSize_ aligned
  // @param placeHandle    handle for data placement technology like FDP
  bool write(uint64_t offset, Buffer buffer, int placeHandle = -1);

  // Write buffer view to the device. This call makes a copy of the buffer if
  // entryptor is present.
  bool write(uint64_t offset, BufferView bufferView, int placeHandle = -1);

  // Allocate a new stream and return the handle for Placement capable devices.
  virtual int allocatePlacementHandle() = 0;

  // Reads @size bytes from device at @deviceOffset and copys to @value
  // There must be sufficient space allocated already in the mutableView.
  // @offset and @size must be ioAligmentSize_ aligned
  // @offset + @size must be less than or equal to device size_
  // address in @value must be ioAligmentSize_ aligned
  bool read(uint64_t offset, uint32_t size, void* value);

  // Reads @size bytes from device at @deviceOffset into a Buffer allocated
  // If the offset is not aligned or size is not aligned for device IO
  // alignment, they both are aligned to do the read operation successfully
  // from the device and then Buffer is adjusted to return only the size
  // bytes from offset.
  Buffer read(uint64_t offset, uint32_t size);

  // Everything should be on device after this call returns.
  void flush() { flushImpl(); }

  // Return bytes written since device start
  uint64_t getBytesWritten() const { return bytesWritten_.get(); }

  // Return bytes read since device start
  uint64_t getBytesRead() const { return bytesRead_.get(); }

  // Export device stats via CounterVisitor
  void getCounters(const CounterVisitor& visitor) const;

  // Returns the size of the device. All IO operations must be from [0, size)
  uint64_t getSize() const { return size_; }

  // Returns the alignment size for device io operations
  uint32_t getIOAlignmentSize() const { return ioAlignmentSize_; }

 protected:
  virtual bool writeImpl(uint64_t offset,
                         uint32_t size,
                         const void* value,
                         int placeHandle = -1) = 0;
  virtual bool readImpl(uint64_t offset, uint32_t size, void* value) = 0;
  virtual void flushImpl() = 0;

  // This measures the latency of an individual read or write iop between its
  // submission and completion. Slowdowns in the kernel and the boundary between
  // kernel and userspace will negatively affect this latency metric. For
  // example: stall during submission, completion callback not invoked
  // immediately after the IO is complete.
  mutable util::PercentileStats readIOOpDeviceLatencyEstimator_;
  mutable util::PercentileStats writeIOOpDeviceLatencyEstimator_;

 private:
  mutable AtomicCounter bytesWritten_;
  mutable AtomicCounter bytesRead_;
  mutable AtomicCounter writeIOErrors_;
  mutable AtomicCounter readIOErrors_;
  mutable AtomicCounter encryptionErrors_;
  mutable AtomicCounter decryptionErrors_;

  // This measures the latency of a read or write request. For synchronous IO,
  // this measures the latency of pread/pwrite. For async IO, this measures
  // the time of creating async io context, any navy async-io queuing delays,
  // and the time spent in the kernel actually scheduling and running the iop.
  mutable util::PercentileStats readLatencyEstimator_;
  mutable util::PercentileStats writeLatencyEstimator_;

  bool readInternal(uint64_t offset, uint32_t size, void* value);

  bool writeInternal(uint64_t offset,
                     const uint8_t* data,
                     size_t size,
                     int placeHandle = -1);

  // size of the device. All offsets for write/read should be contained
  // below this.
  const uint64_t size_{0};

  // alignment granularity for the offsets and size to read/write calls.
  const uint32_t ioAlignmentSize_{kDefaultAlignmentSize};

  // Some devices have this transfer size limit due to DMA size limitations.
  // This limit is applicable for both writes and reads.
  const uint32_t maxIOSize_{0};

  // When write-io is issued, it is broken down into writeImpl calls at
  // this granularity. maxWriteSize_ 0 means no maximum write size.
  // maxWriteSize_ option allows splitting the large writes to smaller
  // writes so that the device read latency is not adversely impacted by
  // large device writes
  const uint32_t maxWriteSize_{0};

  std::shared_ptr<DeviceEncryptor> encryptor_;

  static constexpr uint32_t kDefaultAlignmentSize{1};
};

// Default ioAlignSize size for Memory Device is 1. In our tests, we create
// Devices with different ioAlignSize sizes using memory device. So we need
// a way to set a different ioAlignSize size for memory devices.
std::unique_ptr<Device> createMemoryDevice(
    uint64_t size,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t ioAlignSize = 1);

// Creates a direct IO file device supporting RAID if multiple files are
// provided. If qDepth = 0, sync IO will be used all the time
//
// @param fVec                  vector of file descriptor(s)
// @param filePaths             vector of file path(s)
// @param fileSize              size of the file(s)
// @param blockSize             device block size
// @param stripeSize            RAID stripe size if applicable
// @param maxDeviceWriteSize    device maximum granularity of writes
// @param numIoThreads          the number of IO threads. If 0 and EventBase is
//                              not available, IOs will be fall back to sync IO
// @param ioEngine              IO engine to be used
// @param qDepth                queue depth per each IO thread.
//                              If 0, sync IO will be used
// @param isFDPEnabled          Whether FDP placement mode is enabled or not.
// @param encryptor             encryption object
std::unique_ptr<Device> createDirectIoFileDevice(
    std::vector<folly::File> fVec,
    std::vector<std::string> filePaths,
    uint64_t fileSize,
    uint32_t blockSize,
    uint32_t stripeSize,
    uint32_t maxDeviceWriteSize,
    IoEngine ioEngine,
    uint32_t qDepth,
    bool isFDPEnabled,
    std::shared_ptr<DeviceEncryptor> encryptor);

// A convenient wrapper for creating Device with a sync IO
//
// @param fVec                  vector of file descriptor(s)
// @param fileSize              size of the file(s)
// @param blockSize             device block size
// @param stripeSize            RAID stripe size if applicable
// @param maxDeviceWriteSize    device maximum granularity of writes
// @param encryptor             encryption object
std::unique_ptr<Device> createDirectIoFileDevice(
    std::vector<folly::File> fVec,
    uint64_t fileSize,
    uint32_t blockSize,
    uint32_t stripeSize,
    uint32_t maxDeviceWriteSize,
    std::shared_ptr<DeviceEncryptor> encryptor);

// Creates a direct IO file device.
// RAID0 with given stripe size is applied if multiple files are provided
//
// @param filePaths             name(s) of the file(s)
// @param fileSize              size of the file(s)
// @param truncateFile          whether to truncate the file
// @param blockSize             device block size
// @param stripeSize            RAID stripe size if applicable
// @param maxDeviceWriteSize    device maximum granularity of writes
// @param ioEngine              IoEngine to be used for IO
// @param qDepth                queue depth for async IO; 0 for sync IO
// @param isFDPEnabled          whether FDP placement mode enabled or not
// @param encryptor             encryption object
// @param isExclusiveOwner      fail if not sole owner of the file
std::unique_ptr<Device> createFileDevice(
    std::vector<std::string> filePaths,
    uint64_t fileSize,
    bool truncateFile,
    uint32_t blockSize,
    uint32_t stripeSize,
    uint32_t maxDeviceWriteSize,
    IoEngine ioEngine,
    uint32_t qDepth,
    bool isFDPEnabled,
    std::shared_ptr<navy::DeviceEncryptor> encryptor,
    bool isExclusiveOwner);
} // namespace navy
} // namespace cachelib
} // namespace facebook

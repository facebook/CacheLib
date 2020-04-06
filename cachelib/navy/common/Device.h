#pragma once

#include <folly/io/IOBuf.h>
#include <folly/stats/QuantileEstimator.h>

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
  //                be aligned to the encrytion block size; value must
  //                must be the same in size before and after encryption
  // @param salt    this must be the same salt used later for decryption
  // @return        true if success, false otherwise
  virtual bool encrypt(folly::MutableByteRange value, uint64_t salt) = 0;

  // @param value   will be overwritten with decrypted value; value must
  //                be aligned to the decrytion block size; value must
  //                must be the same in size before and after decryption
  // @param salt    this must be the same earlier used for encryption
  // @return        true if success, false otherwise
  virtual bool decrypt(folly::MutableByteRange value, uint64_t salt) = 0;
};

// Device abstraction
//
// Read/write returns true if @value written/read entirely (all @size bytes).
// Pointer ownership is not passed.
class Device {
 public:
  Device() : Device{nullptr, 0 /* max device write size */} {}

  explicit Device(std::shared_ptr<DeviceEncryptor> encryptor,
                  uint32_t maxWriteSize)
      : encryptor_{std::move(encryptor)}, maxWriteSize_(maxWriteSize) {}

  virtual ~Device() = default;

  // Create an IO buffer of at least @size bytes that can be used for read and
  // write. For example, direct IO device allocates a properly aligned buffer.
  virtual Buffer makeIOBuffer(uint32_t size) = 0;

  // Write buffer to the device. This call takes ownership of the buffer
  // and de-allocates it by end of the call. @buffer must be aligned the same
  // way as `makeIOBuffer` would return.
  bool write(uint64_t offset, Buffer buffer);

  // Reads @size bytes from device at @deviceOffset and copys to @value
  // There must be sufficient space allocated already in the mutableView.
  bool read(uint64_t offset, uint32_t size, void* value);

  void flush() { flushImpl(); }

  uint64_t getBytesWritten() const { return bytesWritten_.get(); }

  void getCounters(const CounterVisitor& visitor) const;

 protected:
  virtual bool writeImpl(uint64_t offset, uint32_t size, const void* value) = 0;
  virtual bool readImpl(uint64_t offset, uint32_t size, void* value) = 0;
  virtual void flushImpl() = 0;

 private:
  mutable AtomicCounter bytesWritten_;
  mutable AtomicCounter writeIOErrors_;
  mutable AtomicCounter readIOErrors_;
  mutable AtomicCounter encryptionErrors_;
  mutable AtomicCounter decryptionErrors_;

  mutable util::PercentileStats readLatencyEstimator_;
  mutable util::PercentileStats writeLatencyEstimator_;

  std::shared_ptr<DeviceEncryptor> encryptor_;
  // maxWriteSize_ 0 means no maximum write size.
  uint32_t maxWriteSize_{0};
};

// Takes ownership of the file descriptor
std::unique_ptr<Device> createFileDevice(
    int fd, std::shared_ptr<DeviceEncryptor> encryptor);
std::unique_ptr<Device> createDirectIoFileDevice(
    int fd,
    uint32_t blockSize,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize);
std::unique_ptr<Device> createDirectIoRAID0Device(
    std::vector<int>& fdvec,
    uint32_t blockSize,
    uint32_t stripeSize,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize);
std::unique_ptr<Device> createMemoryDevice(
    uint64_t size, std::shared_ptr<DeviceEncryptor> encryptor);
} // namespace navy
} // namespace cachelib
} // namespace facebook

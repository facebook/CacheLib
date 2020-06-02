#include <cstring>
#include <numeric>

#include <folly/Format.h>

#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace navy {
namespace {

using IOOperation =
    std::function<ssize_t(int fd, void* buf, size_t count, off_t offset)>;

// Device on Unix file descriptor
class FileDevice final : public Device {
 public:
  FileDevice(int fd,
             uint64_t size,
             uint32_t ioAlignSize,
             std::shared_ptr<DeviceEncryptor> encryptor,
             uint32_t maxDeviceWriteSize)
      : Device{size, std::move(encryptor), ioAlignSize, maxDeviceWriteSize},
        fd_{fd} {}
  FileDevice(const FileDevice&) = delete;
  FileDevice& operator=(const FileDevice&) = delete;

  ~FileDevice() override {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

 private:
  bool writeImpl(uint64_t offset, uint32_t size, const void* value) override {
    bool success = ::pwrite(fd_, value, size, offset) == size;
    if (!success) {
      reportIOError("write", offset, size);
    }
    return success;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    bool success = ::pread(fd_, value, size, offset) == size;
    if (!success) {
      reportIOError("read", offset, size);
    }
    return success;
  }

  void flushImpl() override { ::fsync(fd_); }

  void reportIOError(const char* opName, uint64_t offset, uint32_t size) {
    XLOGF(ERR,
          "IO error: {} offset={} size={} errno={} ({})",
          opName,
          offset,
          size,
          errno,
          std::strerror(errno));
  }

  const int fd_{-1};
};

// RAID0 device spanning multiple files
class RAID0Device final : public Device {
 public:
  RAID0Device(std::vector<int>& fdvec,
              uint64_t size,
              uint32_t ioAlignSize,
              uint32_t stripeSize,
              std::shared_ptr<DeviceEncryptor> encryptor,
              uint32_t maxDeviceWriteSize)
      : Device{size, std::move(encryptor), ioAlignSize, maxDeviceWriteSize},
        fdvec_{fdvec},
        stripeSize_(stripeSize) {
    XDCHECK_GT(ioAlignSize, 0u);
    XDCHECK_GT(stripeSize_, 0u);
    XDCHECK_GE(stripeSize_, ioAlignSize);
  }
  RAID0Device(const RAID0Device&) = delete;
  RAID0Device& operator=(const RAID0Device&) = delete;

  ~RAID0Device() override {
    for (const auto fd : fdvec_) {
      ::close(fd);
    }
  }

 private:
  bool writeImpl(uint64_t offset, uint32_t size, const void* value) override {
    IOOperation io = ::pwrite;
    return doIO(offset, size, const_cast<void*>(value), "RAID0 WRITE", io);
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    IOOperation io = ::pread;
    return doIO(offset, size, value, "RAID0 READ", io);
  }

  void flushImpl() override {
    for (const auto fd : fdvec_) {
      ::fsync(fd);
    }
  }

  bool doIO(uint64_t offset,
            uint32_t size,
            void* value,
            const char* opName,
            IOOperation& io) {
    uint8_t* buf = reinterpret_cast<uint8_t*>(value);

    while (size > 0) {
      uint64_t stripe = offset / stripeSize_;
      uint32_t fdIdx = stripe % fdvec_.size();
      uint64_t stripeStartOffset = (stripe / fdvec_.size()) * stripeSize_;
      uint32_t ioOffsetInStripe = offset % stripeSize_;
      uint32_t allowedIOSize = std::min(size, stripeSize_ - ioOffsetInStripe);

      auto retSize = io(fdvec_[fdIdx],
                        buf,
                        allowedIOSize,
                        stripeStartOffset + ioOffsetInStripe);
      if (retSize != allowedIOSize) {
        reportIOError(opName, offset, size, stripe, ioOffsetInStripe,
                      allowedIOSize);
        return false;
      }

      size -= allowedIOSize;
      offset += allowedIOSize;
      buf += allowedIOSize;
    }
    return true;
  }

  void reportIOError(const char* opName,
                     uint64_t logicalOffset,
                     uint32_t logicalIOSize,
                     uint32_t stripe,
                     uint32_t stripeOffset,
                     uint32_t stripeIOSize) {
    XLOGF(ERR,
          "IO error: {} logicalOffset {} logicalIOSize {} stripeSize {} "
          "stripe {} stripeOffset {} stripeIOSize {} errno {} ({})",
          opName,
          logicalOffset,
          logicalIOSize,
          stripeSize_,
          stripe,
          stripeOffset,
          stripeIOSize,
          errno,
          std::strerror(errno));
  }

  const std::vector<int> fdvec_{};
  const uint32_t stripeSize_{};
};

// Device on memory buffer
class MemoryDevice final : public Device {
 public:
  explicit MemoryDevice(uint64_t size,
                        std::shared_ptr<DeviceEncryptor> encryptor,
                        uint32_t ioAlignSize)
      : Device{size, std::move(encryptor), ioAlignSize,
               0 /* max device write size */},
        buffer_{std::make_unique<uint8_t[]>(size)} {}
  MemoryDevice(const MemoryDevice&) = delete;
  MemoryDevice& operator=(const MemoryDevice&) = delete;
  ~MemoryDevice() override = default;

 private:
  bool writeImpl(uint64_t offset,
                 uint32_t size,
                 const void* value) noexcept override {
    XDCHECK_LE(offset + size, getSize());
    std::memcpy(buffer_.get() + offset, value, size);
    return true;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    XDCHECK_LE(offset + size, getSize());
    std::memcpy(value, buffer_.get() + offset, size);
    return true;
  }

  void flushImpl() override {
    // Noop
  }

  std::unique_ptr<uint8_t[]> buffer_;
};
} // namespace

bool Device::write(uint64_t offset, Buffer buffer) {
  const auto size = buffer.size();
  XDCHECK_LE(offset + buffer.size(), size_);
  void* data = buffer.data();
  XDCHECK_EQ(reinterpret_cast<uint64_t>(data) % ioAlignmentSize_, 0ul);
  if (encryptor_) {
    XCHECK_EQ(offset % encryptor_->encryptionBlockSize(), 0ul);
    auto res = encryptor_->encrypt(
        folly::MutableByteRange{reinterpret_cast<uint8_t*>(data), size},
        offset);
    if (!res) {
      encryptionErrors_.inc();
      return false;
    }
  }

  auto timeBegin = getSteadyClock();
  auto remainingSize = size;
  auto maxWriteSize = (maxWriteSize_ == 0) ? remainingSize : maxWriteSize_;
  bool result = true;
  while (remainingSize > 0) {
    auto writeSize = std::min<size_t>(maxWriteSize, remainingSize);
    XDCHECK_EQ(offset % ioAlignmentSize_, 0ul);
    XDCHECK_EQ(writeSize % ioAlignmentSize_, 0ul);
    result = writeImpl(offset, writeSize, data);
    if (!result) {
      break;
    }
    bytesWritten_.add(result * size);
    offset += writeSize;
    remainingSize -= writeSize;
  }
  if (!result) {
    writeIOErrors_.inc();
  }
  writeLatencyEstimator_.trackValue(
      toMicros((getSteadyClock() - timeBegin)).count());
  return result;
}

// reads size number of bytes from the device from the offset into value.
// Both offset and size are expected to be aligned for device IO operations.
// If successful and encryptor_ is defined, size bytes from
// validDataOffsetInValue offset in value are decrypted.
//
// returns true if successful, false otherwise.
bool Device::readInternal(uint64_t offset, uint32_t size, void* value) {
  XDCHECK_EQ(reinterpret_cast<uint64_t>(value) % ioAlignmentSize_, 0ul);
  XDCHECK_EQ(offset % ioAlignmentSize_, 0ul);
  XDCHECK_EQ(size % ioAlignmentSize_, 0ul);
  XDCHECK_LE(offset + size, size_);
  auto timeBegin = getSteadyClock();
  bool result = readImpl(offset, size, value);
  readLatencyEstimator_.trackValue(
      toMicros(getSteadyClock() - timeBegin).count());
  if (!result) {
    readIOErrors_.inc();
    return result;
  }

  if (encryptor_) {
    XCHECK_EQ(offset % encryptor_->encryptionBlockSize(), 0ul);
    auto res = encryptor_->decrypt(
        folly::MutableByteRange{reinterpret_cast<uint8_t*>(value), size},
        offset);
    if (!res) {
      decryptionErrors_.inc();
      return false;
    }
  }
  return true;
}

// This API reads size bytes from the Device from offset in to a Buffer and
// returns the Buffer. If offset and size are not aligned to device's
// ioAlignmentSize_, IO aligned offset and IO aligned size are determined
// and passed to device read. Upon successful read from the device, the
// buffer is adjusted to return the intended data by trimming the data in
// the front and back.
// An empty buffer is returned in case of error and the caller must check
// the buffer size returned with size passed in to check for errors.
Buffer Device::read(uint64_t offset, uint32_t size) {
  XDCHECK_LE(offset + size, size_);
  uint64_t readOffset =
      offset & ~(static_cast<uint64_t>(ioAlignmentSize_) - 1ul);
  uint64_t readPrefixSize =
      offset & (static_cast<uint64_t>(ioAlignmentSize_) - 1ul);
  auto readSize = getIOAlignedSize(readPrefixSize + size);
  auto buffer = makeIOBuffer(readSize);
  bool result = readInternal(readOffset, readSize, buffer.data());
  if (!result) {
    return Buffer{};
  }
  buffer.trimStart(readPrefixSize);
  buffer.shrink(size);
  return buffer;
}

// This API reads size bytes from the Device from the offset offset into
// value. Both offset and size are expected to be IO aligned.
bool Device::read(uint64_t offset, uint32_t size, void* value) {
  return readInternal(offset, size, value);
}

void Device::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_device_bytes_written", getBytesWritten());
  readLatencyEstimator_.visitQuantileEstimator(visitor, "{}_us_{}",
                                               "navy_device_read_latency");
  writeLatencyEstimator_.visitQuantileEstimator(visitor, "{}_us_{}",
                                                "navy_device_write_latency");
  visitor("navy_device_read_errors", readIOErrors_.get());
  visitor("navy_device_write_errors", writeIOErrors_.get());
  visitor("navy_device_encryption_errors", encryptionErrors_.get());
  visitor("navy_device_decryption_errors", decryptionErrors_.get());
}

std::unique_ptr<Device> createFileDevice(
    int fd, uint64_t size, std::shared_ptr<DeviceEncryptor> encryptor) {
  return std::make_unique<FileDevice>(fd, size, 0, std::move(encryptor),
                                      0 /* max device write size */);
}

std::unique_ptr<Device> createDirectIoFileDevice(
    int fd,
    uint64_t size,
    uint32_t ioAlignSize,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize) {
  XDCHECK(folly::isPowTwo(ioAlignSize));
  return std::make_unique<FileDevice>(fd, size, ioAlignSize,
                                      std::move(encryptor), maxDeviceWriteSize);
}

std::unique_ptr<Device> createDirectIoRAID0Device(
    std::vector<int>& fdvec,
    uint64_t size, // size of each device in the RAID
    uint32_t ioAlignSize,
    uint32_t stripeSize,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize) {
  XDCHECK(folly::isPowTwo(ioAlignSize));
  return std::make_unique<RAID0Device>(fdvec, fdvec.size() * size, ioAlignSize,
                                       stripeSize, std::move(encryptor),
                                       maxDeviceWriteSize);
}

std::unique_ptr<Device> createMemoryDevice(
    uint64_t size,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t ioAlignSize) {
  return std::make_unique<MemoryDevice>(size, std::move(encryptor),
                                        ioAlignSize);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

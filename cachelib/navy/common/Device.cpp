#include "Device.h"

#include <unistd.h>
#include <cstring>

#include <folly/Format.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace {

using IOOperation =
    std::function<ssize_t(int fd, void* buf, size_t count, off_t offset)>;

// Device on Unix file descriptor
class FileDevice final : public Device {
 public:
  explicit FileDevice(int fd) : fd_{fd} {}
  // Overload for a raw device
  FileDevice(int fd, uint32_t blockSize)
      : fd_{fd}, raw_{true}, blockSize_{blockSize} {
    XDCHECK_GT(blockSize_, 0u);
  }
  FileDevice(const FileDevice&) = delete;
  FileDevice& operator=(const FileDevice&) = delete;

  ~FileDevice() override {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  Buffer makeIOBuffer(uint32_t size) override {
    if (raw_) {
      return Buffer{powTwoAlign(size, blockSize_), blockSize_};
    } else {
      return Buffer{size};
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
  const bool raw_{false};
  const uint32_t blockSize_{};
};

// RAID0 device spanning multiple files
class RAID0Device final : public Device {
 public:
  RAID0Device(std::vector<int>& fdvec, uint32_t blockSize, uint32_t stripeSize)
      : fdvec_{fdvec},
        raw_{true},
        blockSize_{blockSize},
        stripeSize_(stripeSize) {
    XDCHECK_GT(blockSize_, 0u);
    XDCHECK_GT(stripeSize_, 0u);
    XDCHECK_GE(stripeSize_, blockSize_);
  }
  RAID0Device(const RAID0Device&) = delete;
  RAID0Device& operator=(const RAID0Device&) = delete;

  ~RAID0Device() override {
    for (const auto fd : fdvec_) {
      ::close(fd);
    }
  }

  Buffer makeIOBuffer(uint32_t size) override {
    if (raw_) {
      return Buffer{powTwoAlign(size, blockSize_), blockSize_};
    } else {
      return Buffer{size};
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
        reportIOError(
            opName, offset, size, stripe, ioOffsetInStripe, allowedIOSize);
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
          "stripe {} stripeOffset {} stripeIOSize {} errno %i ({})",
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
  const bool raw_{false};
  const uint32_t blockSize_{};
  const uint32_t stripeSize_{};
};

// Device on memory buffer
class MemoryDevice final : public Device {
 public:
  explicit MemoryDevice(uint64_t size)
      : size_{size}, buffer_{std::make_unique<uint8_t[]>(size)} {}
  MemoryDevice(const MemoryDevice&) = delete;
  MemoryDevice& operator=(const MemoryDevice&) = delete;
  ~MemoryDevice() override = default;

  Buffer makeIOBuffer(uint32_t size) override { return Buffer{size}; }

 private:
  bool writeImpl(uint64_t offset,
                 uint32_t size,
                 const void* value) noexcept override {
    XDCHECK_LE(offset + size, size_);
    std::memcpy(buffer_.get() + offset, value, size);
    return true;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    XDCHECK_LE(offset + size, size_);
    (void)size_;
    std::memcpy(value, buffer_.get() + offset, size);
    return true;
  }

  void flushImpl() override {
    // Noop
  }

  const uint64_t size_{};
  std::unique_ptr<uint8_t[]> buffer_;
};

void visitQuantileEstimator(const CounterVisitor& visitor,
                            folly::SlidingWindowQuantileEstimator<>& qe,
                            folly::StringPiece prefix) {
  qe.flush();
  static const std::array<const char*, 8> kStatNames{
      "p50", "p90", "p99", "p999", "p9999", "p99999", "p999999", "p100"};
  static const std::array<double, 8> kQuantiles{
      0.5, 0.9, 0.99, 0.999, 0.9999, 0.99999, 0.999999, 1.0};
  auto q = qe.estimateQuantiles(
      folly::Range<const double*>(kQuantiles.begin(), kQuantiles.end()));
  XDCHECK_EQ(q.quantiles.size(), kQuantiles.size());
  for (size_t i = 0; i < kQuantiles.size(); i++) {
    auto p = folly::sformat("{}_us_{}", prefix, kStatNames[i]);
    visitor(p, q.quantiles[i].second);
  }
}
} // namespace

constexpr int Device::kDefaultWindowSize;

void Device::getCounters(const CounterVisitor& visitor) const {
  visitor("navy_device_bytes_written", getBytesWritten());
  visitQuantileEstimator(
      visitor, readLatencyEstimator_, "navy_device_read_latency");
  visitQuantileEstimator(
      visitor, writeLatencyEstimator_, "navy_device_write_latency");
  visitor("navy_device_read_errors", readIOErrors_.get());
  visitor("navy_device_write_errors", writeIOErrors_.get());
}

std::unique_ptr<Device> createFileDevice(int fd) {
  return std::make_unique<FileDevice>(fd, 0);
}

std::unique_ptr<Device> createDirectIoFileDevice(int fd, uint32_t blockSize) {
  XDCHECK(folly::isPowTwo(blockSize));
  return std::make_unique<FileDevice>(fd, blockSize);
}

std::unique_ptr<Device> createDirectIoRAID0Device(std::vector<int>& fdvec,
                                                  uint32_t blockSize,
                                                  uint32_t stripeSize) {
  XDCHECK(folly::isPowTwo(blockSize));
  return std::make_unique<RAID0Device>(fdvec, blockSize, stripeSize);
}

std::unique_ptr<Device> createMemoryDevice(uint64_t size) {
  return std::make_unique<MemoryDevice>(size);
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

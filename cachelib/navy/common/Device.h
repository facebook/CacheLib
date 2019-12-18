#pragma once

#include <folly/stats/QuantileEstimator.h>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Device abstraction
//
// Read/write returns true if @value written/read entirely (all @size bytes).
// Pointer ownership is not passed.
class Device {
 public:
  Device()
      : readLatencyEstimator_{std::chrono::seconds{kDefaultWindowSize}},
        writeLatencyEstimator_{std::chrono::seconds{kDefaultWindowSize}} {}

  virtual ~Device() = default;

  // Create an IO buffer of at least @size bytes that can be used for read and
  // write. For example, direct IO device allocates a properly aligned buffer.
  virtual Buffer makeIOBuffer(uint32_t size) = 0;

  // Copys data of size @size from @value to device @offset
  bool write(uint64_t offset, uint32_t size, const void* value) {
    auto timeBegin = getSteadyClock();
    bool result = writeImpl(offset, size, value);
    bytesWritten_.add(result * size);
    if (!result) {
      writeIOErrors_.inc();
    }
    writeLatencyEstimator_.addValue(
        toMicros((getSteadyClock() - timeBegin)).count());
    return result;
  }

  // Reads @size bytes from device at @deviceOffset and copys to @value
  bool read(uint64_t offset, uint32_t size, void* value) {
    auto timeBegin = getSteadyClock();
    bool result = readImpl(offset, size, value);
    readLatencyEstimator_.addValue(
        toMicros(getSteadyClock() - timeBegin).count());
    if (!result) {
      readIOErrors_.inc();
    }
    return result;
  }

  void flush() { flushImpl(); }

  uint64_t getBytesWritten() const { return bytesWritten_.get(); }

  void getCounters(const CounterVisitor& visitor) const;

 protected:
  virtual bool writeImpl(uint64_t offset, uint32_t size, const void* value) = 0;
  virtual bool readImpl(uint64_t offset, uint32_t size, void* value) = 0;
  virtual void flushImpl() = 0;

 private:
  static constexpr int kDefaultWindowSize = 1;

  mutable AtomicCounter bytesWritten_;
  mutable AtomicCounter writeIOErrors_;
  mutable AtomicCounter readIOErrors_;
  mutable folly::SlidingWindowQuantileEstimator<> readLatencyEstimator_;
  mutable folly::SlidingWindowQuantileEstimator<> writeLatencyEstimator_;
};

// Takes ownership of the file descriptor
std::unique_ptr<Device> createFileDevice(int fd);
std::unique_ptr<Device> createDirectIoFileDevice(int fd, uint32_t blockSize);
std::unique_ptr<Device> createDirectIoRAID0Device(std::vector<int>& fdvec,
                                                  uint32_t blockSize,
                                                  uint32_t stripeSize);
std::unique_ptr<Device> createMemoryDevice(uint64_t size);
} // namespace navy
} // namespace cachelib
} // namespace facebook

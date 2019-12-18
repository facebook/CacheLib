#pragma once

#include "cachelib/navy/common/Device.h"

#include <cassert>
#include <memory>

#include <gmock/gmock.h>

namespace facebook {
namespace cachelib {
namespace navy {
class MockDevice : public Device {
 public:
  // If @deviceSize is 0, constructor doesn't create a memory device.
  // User should set it manually.
  MockDevice(uint32_t deviceSize, uint32_t blockSize);

  Buffer makeIOBuffer(uint32_t size) override {
    // Implementation expected to request blocks multiple of block size
    XDCHECK_EQ(size % blockSize_, 0u);
    return Buffer{size, blockSize_};
  }

  MOCK_METHOD3(readImpl, bool(uint64_t, uint32_t, void*));
  MOCK_METHOD3(writeImpl, bool(uint64_t, uint32_t, const void*));
  MOCK_METHOD0(flushImpl, void());

  Device& getRealDeviceRef() { return *device_; }

  void setRealDevice(std::unique_ptr<Device> device) {
    device_ = std::move(device);
  }

  std::unique_ptr<Device> releaseRealDevice() { return std::move(device_); }

 private:
  const uint32_t blockSize_{};
  std::unique_ptr<Device> device_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

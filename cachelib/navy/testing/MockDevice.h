#pragma once

#include <gmock/gmock.h>

#include <cassert>
#include <memory>

#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace navy {
class MockDevice : public Device {
 public:
  // If @deviceSize is 0, constructor doesn't create a memory device.
  // User should set it manually.
  //
  // @param deviceSize    size of the device
  // @param ioAlignSize   alignment size for IO operations
  // @param encryptor     encryption object
  MockDevice(uint64_t deviceSize,
             uint32_t ioAlignSize,
             std::shared_ptr<DeviceEncryptor> encryptor = nullptr);

  MOCK_METHOD3(readImpl, bool(uint64_t, uint32_t, void*));
  MOCK_METHOD3(writeImpl, bool(uint64_t, uint32_t, const void*));
  MOCK_METHOD0(flushImpl, void());

  Device& getRealDeviceRef() { return *device_; }

  void setRealDevice(std::unique_ptr<Device> device) {
    device_ = std::move(device);
  }

  std::unique_ptr<Device> releaseRealDevice() { return std::move(device_); }

 private:
  std::unique_ptr<Device> device_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

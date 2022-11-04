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

#include <gmock/gmock.h>

#include <cassert>
#include <memory>

#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Mock device implements the Device API and internally has a real
// device of user's choosing. This is used in unit tests where we
// want to assert certain behavior in scenarios that invovle devices.
class MockDevice : public Device {
 public:
  // If @deviceSize is 0, constructor doesn't create a memory device.
  // User should set it manually after construction.
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

  // Returns pointer to the device backing this mock object. This is
  // useful if user wants to bypass the mock to access the real device
  // during a test. For example, write some corrupted data, and then use
  // mock to verify read failure scenarios.
  Device& getRealDeviceRef() { return *device_; }

  // Sets a new real device backing this mock object
  void setRealDevice(std::unique_ptr<Device> device) {
    device_ = std::move(device);
  }

  // Detaches the real device from the mock object
  std::unique_ptr<Device> releaseRealDevice() { return std::move(device_); }

 private:
  std::unique_ptr<Device> device_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

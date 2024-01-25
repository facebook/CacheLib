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

#include "cachelib/navy/testing/MockDevice.h"

namespace facebook {
namespace cachelib {
namespace navy {
MockDevice::MockDevice(uint64_t deviceSize,
                       uint32_t ioAlignSize,
                       std::shared_ptr<DeviceEncryptor> encryptor)
    : Device{deviceSize, nullptr, ioAlignSize, 0, 0},
      device_{deviceSize == 0
                  ? nullptr
                  : createMemoryDevice(
                        deviceSize, std::move(encryptor), ioAlignSize)} {
  ON_CALL(*this, readImpl(testing::_, testing::_, testing::_))
      .WillByDefault(
          testing::Invoke([this](uint64_t offset, uint32_t size, void* buffer) {
            XDCHECK_EQ(size % getIOAlignmentSize(), 0u);
            XDCHECK_EQ(offset % getIOAlignmentSize(), 0u);
            return device_->read(offset, size, buffer);
          }));

  ON_CALL(*this, writeImpl(testing::_, testing::_, testing::_, testing::_))
      .WillByDefault(testing::Invoke(
          [this](uint64_t offset, uint32_t size, const void* data, int) {
            XDCHECK_EQ(size % getIOAlignmentSize(), 0u);
            XDCHECK_EQ(offset % getIOAlignmentSize(), 0u);
            Buffer buffer = device_->makeIOBuffer(size);
            std::memcpy(buffer.data(), data, size);
            return device_->write(offset, std::move(buffer));
          }));

  ON_CALL(*this, flushImpl()).WillByDefault(testing::Invoke([this]() {
    device_->flush();
  }));

  ON_CALL(*this, allocatePlacementHandle()).WillByDefault(testing::Invoke([]() {
    return -1;
  }));
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

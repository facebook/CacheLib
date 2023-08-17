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

#include <memory>

#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace navy {

// Create a Device that supports async IO.
//
// @param f the file descriptor of the device
// @param size the size of the device in bytes
// @param blockSize the size of each block in bytes
// @param numIoThreads the number of IO threads to be created
// @param qDepthPerThread the queue depth per IO thread
// @param encryptor the DeviceEncryptor for data encryption
// @param maxDeviceWriteSize the maximum write size to the device
std::unique_ptr<Device> createAsyncIoFileDevice(
    folly::File f,
    uint64_t size,
    uint32_t blockSize,
    uint32_t numIoThreads,
    uint32_t qDepthPerThread,
    std::shared_ptr<DeviceEncryptor> encryptor,
    uint32_t maxDeviceWriteSize,
    bool enableIoUring);
} // namespace navy
} // namespace cachelib
} // namespace facebook

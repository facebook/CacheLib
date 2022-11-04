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

#include <folly/io/IOBufQueue.h>

#include <memory>
#include <stdexcept>

#include "cachelib/common/Serialization.h"
#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace navy {
// @param dev           The device the record writer will serialize to
// @param metadataSize  Reserved space on the device for the serialized metadata
std::unique_ptr<RecordWriter> createMetadataRecordWriter(Device& dev,
                                                         size_t metadataSize);

// @param dev           The device the record reader will deserialize from
// @param metadataSize  Reserved space on the device for the serialized metadata
std::unique_ptr<RecordReader> createMetadataRecordReader(Device& dev,
                                                         size_t metadataSize);

// @param fd    The file the record writer will serialize to
std::unique_ptr<RecordWriter> createFileRecordWriter(int fd);

// @param fd    The file the record reader will deserialize from
std::unique_ptr<RecordReader> createFileRecordReader(int fd);

// @param fd    The file the record writer will serialize to
std::unique_ptr<RecordWriter> createFileRecordWriter(folly::File file);

// @param fd    The file the record reader will deserialize from
std::unique_ptr<RecordReader> createFileRecordReader(folly::File file);
} // namespace navy
} // namespace cachelib
} // namespace facebook

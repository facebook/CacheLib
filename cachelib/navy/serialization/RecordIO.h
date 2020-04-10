#pragma once

#include <memory>
#include <stdexcept>

#include <folly/io/IOBufQueue.h>

#include "cachelib/common/Serialization.h"
#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace navy {

std::unique_ptr<RecordWriter> createMetadataRecordWriter(Device& dev,
                                                         size_t metadataSize);
std::unique_ptr<RecordReader> createMetadataRecordReader(Device& dev,
                                                         size_t metadataSize);
std::unique_ptr<RecordWriter> createFileRecordWriter(int fd);
std::unique_ptr<RecordReader> createFileRecordReader(int fd);
std::unique_ptr<RecordWriter> createMemoryRecordWriter(
    folly::IOBufQueue& ioQueue);
std::unique_ptr<RecordReader> createMemoryRecordReader(
    folly::IOBufQueue& ioQueue);
} // namespace navy
} // namespace cachelib
} // namespace facebook

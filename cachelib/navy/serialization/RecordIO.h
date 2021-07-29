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
} // namespace navy
} // namespace cachelib
} // namespace facebook

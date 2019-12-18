#pragma once

#include <memory>
#include <stdexcept>

#include <folly/io/IOBufQueue.h>

#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace navy {
constexpr uint32_t kBlockSize = 4096;
// Abstract record reader/writer interfaces. Read and write functions throw
// std::runtime_error in case of errors.
class RecordWriter {
 public:
  virtual ~RecordWriter() = default;
  virtual void writeRecord(std::unique_ptr<folly::IOBuf> buf) = 0;
  virtual bool invalidate() = 0;
};

class RecordReader {
 public:
  virtual ~RecordReader() = default;
  virtual std::unique_ptr<folly::IOBuf> readRecord() = 0;
  virtual bool isEnd() const = 0;
};

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

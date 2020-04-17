#include "cachelib/common/Serialization.h"

namespace facebook {
namespace cachelib {
Serializer::Serializer(uint8_t* begin, const uint8_t* const end)
    : curr_(begin), end_(end) {}

size_t Serializer::bytesRemaining() const noexcept {
  XDCHECK_GE(reinterpret_cast<uintptr_t>(end_),
             reinterpret_cast<uintptr_t>(curr_));
  return end_ - curr_;
}

size_t Serializer::writeToBuffer(std::unique_ptr<folly::IOBuf> ioBuf) {
  XDCHECK_LT(reinterpret_cast<uintptr_t>(curr_),
             reinterpret_cast<uintptr_t>(end_));
  if (!ioBuf) {
    throw std::invalid_argument("IOBuf is nullptr");
  }
  ioBuf->coalesce(); // coalesece the chain of IOBufs into one single IOBuf
  const auto length = ioBuf->length();
  if (bytesRemaining() < length) {
    throw std::length_error(
        folly::sformat("Buffer insufficient for serialization."
                       "Has {} bytes left. Need {} bytes.",
                       bytesRemaining(), length));
  }
  memcpy(curr_, ioBuf->data(), length);
  curr_ += length;
  return length;
}

Deserializer::Deserializer(const uint8_t* begin, const uint8_t* const end)
    : curr_(begin), end_(end) {}

size_t Deserializer::bytesRemaining() const noexcept {
  XDCHECK_GE(reinterpret_cast<uintptr_t>(end_),
             reinterpret_cast<uintptr_t>(curr_));
  return end_ - curr_;
}

namespace {
class MemoryRecordWriter final : public RecordWriter {
 public:
  explicit MemoryRecordWriter(folly::IOBufQueue& ioQueue) : ioQueue_{ioQueue} {}
  ~MemoryRecordWriter() override = default;

  void writeRecord(std::unique_ptr<folly::IOBuf> buf) override {
    buf->coalesce();
    ioQueue_.append(std::move(buf));
  }
  bool invalidate() override { return false; }

 private:
  folly::IOBufQueue& ioQueue_;
};

class MemoryRecordReader final : public RecordReader {
 public:
  explicit MemoryRecordReader(folly::IOBufQueue& ioQueue) : ioQueue_{ioQueue} {}
  ~MemoryRecordReader() override = default;

  std::unique_ptr<folly::IOBuf> readRecord() override {
    return ioQueue_.pop_front();
  }

  bool isEnd() const override { return ioQueue_.empty(); }

 private:
  folly::IOBufQueue& ioQueue_;
};
} // namespace

std::unique_ptr<RecordWriter> createMemoryRecordWriter(
    folly::IOBufQueue& ioQueue) {
  return std::make_unique<MemoryRecordWriter>(ioQueue);
}

std::unique_ptr<RecordReader> createMemoryRecordReader(
    folly::IOBufQueue& ioQueue) {
  return std::make_unique<MemoryRecordReader>(ioQueue);
}

} // namespace cachelib
} // namespace facebook

#include "cachelib/common/Serialization.h"

using namespace facebook::cachelib;

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

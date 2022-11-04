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
#include <folly/logging/xlog.h>

#include <cstddef>
#include <stdexcept>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <thrift/lib/cpp2/protocol/Serializer.h>
#pragma GCC diagnostic pop

namespace facebook {
namespace cachelib {

// datastructure that helps serialize and deserialize state of objects in
// Slab*.h
class Serializer {
 public:
  // create a serializer with the given buffer range.
  Serializer(uint8_t* begin, const uint8_t* const end);

  // number of bytes available for any more serialize() calls.
  size_t bytesRemaining() const noexcept;

  // T must be a thrift object advances the buffer only on successful
  // serialization.
  //
  // @param object  the object to be serialized
  //
  // @return  the number of bytes consumed in the buffer to serialize the
  //          object
  //
  // @throw std::exception with appropriate message.
  template <typename T>
  size_t serialize(const T& object);

  // Write content from an IOBuf to the serializer's buffer
  //
  // If the user intends to reuse the IOBuf after this call, copy the
  // content of IOBuf and then pass that copy into this function.
  //
  // @param ioBuf  the iobuf to be written to the buffer.
  //               the memory used by iobuf might change because internally
  //               we coalesce the entire chain of iobuf's into one single
  //               iobuf.
  //
  // @return  the number of bytes consumed in the buffer
  //
  // @throw std::exception with appropriate message.
  size_t writeToBuffer(std::unique_ptr<folly::IOBuf> ioBuf);

  // Serialize the object into an IOBuf
  //
  // This should be used if the user isn't sure how much space the serialized
  // object will take:
  //  1. Serialize the object into IOBuf
  //  2. Create a Serializer object of appropriate size with respect to the
  //     length of the IOBuf
  //  3. Call serializer.writeToBuffer
  //
  // @param object  the object to be serialized
  //
  // @return std::unique_ptr<folly::IOBuf>  that contains the serialized data
  template <typename T, typename Protocol = apache::thrift::BinarySerializer>
  static std::unique_ptr<folly::IOBuf> serializeToIOBuf(const T& object);

  // Serialize the object into an IOBuf queue. Same as the above, but the
  // caller passes the queue.
  template <typename T, typename Protocol = apache::thrift::BinarySerializer>
  static void serializeToIOBufQueue(folly::IOBufQueue& queue, const T& object);

 private:
  uint8_t* curr_;
  const uint8_t* const end_;
};

class Deserializer {
 public:
  // create a deserializer with the given byte range
  Deserializer(const uint8_t* begin, const uint8_t* const end);

  // number of bytes remaining for deserialize() calls
  size_t bytesRemaining() const noexcept;

  // T must be a thrift object. Advances the buffer only on successful
  // deserialization.
  //
  // @throw std::exception with appropriate error message.
  template <typename T, typename Protocol = apache::thrift::BinarySerializer>
  T deserialize();

  // same as the above.
  //
  // @param  object  the object that will hold the deserialized state.
  // @return  the number of bytes consumed in the buffer to serialize the
  //          object
  template <typename T, typename Protocol = apache::thrift::BinarySerializer>
  size_t deserialize(T& object);

 private:
  const uint8_t* curr_;
  const uint8_t* const end_;
};

// implementation
template <typename T>
size_t Serializer::serialize(const T& object) {
  return writeToBuffer(serializeToIOBuf(object));
}

template <typename T, typename Protocol>
std::unique_ptr<folly::IOBuf> Serializer::serializeToIOBuf(const T& object) {
  folly::IOBufQueue queue;
  serializeToIOBufQueue<T, Protocol>(queue, object);
  auto ioBuf = queue.move();
  ioBuf->coalesce();
  return ioBuf;
}

template <typename T, typename Protocol>
void Serializer::serializeToIOBufQueue(folly::IOBufQueue& queue,
                                       const T& object) {
  Protocol::serialize(object, &queue);
}

template <typename T, typename Protocol>
T Deserializer::deserialize() {
  XDCHECK_LT(reinterpret_cast<uintptr_t>(curr_),
             reinterpret_cast<uintptr_t>(end_));
  T object;
  deserialize<T, Protocol>(object);
  return object;
}

template <typename T, typename Protocol>
size_t Deserializer::deserialize(T& object) {
  folly::ByteRange buffer(curr_, end_);
  // throws apache::thrift::protocol::TProtocolException which inherits from
  // std::exception and has the appropriate what() message.
  const auto bytesRead = Protocol::deserialize(buffer, object);
  curr_ += bytesRead;
  return bytesRead;
}

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

// record reader and write implemmentations that use a folly io queue as
// backing medium.
std::unique_ptr<RecordWriter> createMemoryRecordWriter(
    folly::IOBufQueue& ioQueue);
std::unique_ptr<RecordReader> createMemoryRecordReader(
    folly::IOBufQueue& ioQueue);

template <typename ThriftObject, typename SerializationProto>
void serializeProto(const ThriftObject& obj, RecordWriter& writer) {
  folly::IOBufQueue temp;
  SerializationProto::template serialize(obj, &temp);
  // Passes linked chain of IOBufs
  writer.writeRecord(temp.move());
}

template <typename ThriftObject, typename SerializationProto>
ThriftObject deserializeProto(RecordReader& reader) {
  ThriftObject obj;
  auto buf = reader.readRecord();
  SerializationProto::template deserialize<ThriftObject>(buf.get(), obj);
  return obj;
}

template <typename ThriftObject>
std::string serializeToJson(const ThriftObject& obj) {
  return apache::thrift::SimpleJSONSerializer::serialize<std::string>(obj);
}

} // namespace cachelib
} // namespace facebook

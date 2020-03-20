#pragma once

#include "cachelib/navy/serialization/RecordIO.h"
#include "cachelib/navy/serialization/gen-cpp2/objects_types.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

namespace facebook {
namespace cachelib {
namespace navy {
using ProtoSerializer = apache::thrift::BinarySerializer;

template <typename ThriftObject>
void serializeProto(const ThriftObject& obj, RecordWriter& writer) {
  folly::IOBufQueue temp;
  ProtoSerializer::serialize(obj, &temp);
  // Passes linked chain of IOBufs
  writer.writeRecord(temp.move());
}

template <typename ThriftObject>
ThriftObject deserializeProto(RecordReader& reader) {
  ThriftObject obj;
  auto buf = reader.readRecord();
  ProtoSerializer::deserialize<ThriftObject>(buf.get(), obj);
  return obj;
}

template <typename ThriftObject>
std::string serializeToJson(const ThriftObject& obj) {
  std::string tmp;
  ProtoSerializer::serialize(obj,&tmp);
  return tmp;
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

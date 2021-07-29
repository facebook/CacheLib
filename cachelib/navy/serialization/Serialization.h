#pragma once

#include "cachelib/common/Serialization.h"
#include "cachelib/navy/serialization/gen-cpp2/objects_types.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

namespace facebook {
namespace cachelib {
namespace navy {
using ProtoSerializer = apache::thrift::BinarySerializer;

// @param obj       Object to be serialized. Must be a thrift object
// @param writer    Serializer that implements RecordWriter interface
template <typename ThriftObject>
void serializeProto(const ThriftObject& obj, RecordWriter& writer) {
  facebook::cachelib::serializeProto<ThriftObject, ProtoSerializer>(obj,
                                                                    writer);
}

// @param reader    Deserializer that implements RecordReader interface
// @return  deserialized thrift object
template <typename ThriftObject>
ThriftObject deserializeProto(RecordReader& reader) {
  return facebook::cachelib::deserializeProto<ThriftObject, ProtoSerializer>(
      reader);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

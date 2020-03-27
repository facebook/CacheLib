#include <gtest/gtest.h>

#include "cachelib/allocator/memory/serialize/gen-cpp2/memory_objects_types.h"
#include "cachelib/common/Serialization.h"

constexpr size_t SerializationBufferSize = 100 * 1024;
using namespace facebook::cachelib;
TEST(Serializer, InvalidSerialization) {
  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serialization::MemoryPoolManagerObject m;
  serializer.serialize(m);
  const size_t bytesUsed =
      SerializationBufferSize - serializer.bytesRemaining();

  // test serializing with smaller buffer than what is required and it must
  // throw
  {
    const size_t smallSize = bytesUsed / 2;
    ASSERT_LT(smallSize, bytesUsed);
    uint8_t smallBuf[smallSize];
    Serializer s2(&smallBuf[0], &smallBuf[smallSize - 1]);
    ASSERT_THROW(s2.serialize(m), std::exception);
  }
}

TEST(Deserializer, InvaliDeSerialization) {
  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serialization::MemoryPoolManagerObject m;
  serializer.serialize(m);
  const size_t bytesUsed =
      SerializationBufferSize - serializer.bytesRemaining();

  const size_t smallSize = bytesUsed / 2;
  ASSERT_LT(smallSize, bytesUsed);
  Deserializer deserializer(begin, begin + smallSize);
  ASSERT_THROW(deserializer.deserialize<serialization::MemoryAllocatorObject>(),
               std::exception);
}

TEST(Serializer, BytesConsumed) {
  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serialization::MemoryPoolManagerObject m;
  const auto bytesUsed = serializer.serialize(m);
  ASSERT_EQ(bytesUsed, SerializationBufferSize - serializer.bytesRemaining());
}

TEST(Deserializer, BytesConsumed) {
  uint8_t buffer[SerializationBufferSize];
  uint8_t* begin = buffer;
  uint8_t* end = buffer + SerializationBufferSize;
  Serializer serializer(begin, end);
  serialization::MemoryPoolManagerObject m;
  const auto bytesUsed = serializer.serialize(m);

  Deserializer deserializer(begin, begin + bytesUsed);
  serialization::MemoryPoolManagerObject m1;
  auto bytesUsed1 = deserializer.deserialize(m1);
  ASSERT_EQ(bytesUsed, bytesUsed1);
  ASSERT_EQ(0, deserializer.bytesRemaining());
}

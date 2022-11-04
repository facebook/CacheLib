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

#include <gtest/gtest.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include "cachelib/allocator/memory/serialize/gen-cpp2/objects_types.h"
#pragma GCC diagnostic pop

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

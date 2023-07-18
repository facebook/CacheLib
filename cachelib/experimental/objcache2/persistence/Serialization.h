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

#include "cachelib/common/Serialization.h"

namespace facebook {
namespace cachelib {
namespace objcache2 {

// Data structure to be passed in ObjectCache::SerializeCb.
template <typename ObjectCache>
struct ObjectSerializer {
  using Key = typename ObjectCache::Key;

  explicit ObjectSerializer(Key key, uintptr_t objectPtr)
      : key(key), objectPtr(objectPtr) {}

  // Serialize the object of type T into an IOBuf.
  // T must be a Thrift object.
  template <typename T>
  std::unique_ptr<folly::IOBuf> serialize() {
    return Serializer::serializeToIOBuf(*reinterpret_cast<T*>(objectPtr));
  }

  // Serialize the object of type T into an IOBuf.
  // T is a non-Thrift object.
  //
  // @param toThriftCb  callback converting a non-Thrift object to a Thrift
  //                    object
  template <typename T, typename ThriftT>
  std::unique_ptr<folly::IOBuf> serialize(
      std::function<ThriftT(T*)> toThriftCb) {
    auto thriftObj = toThriftCb(reinterpret_cast<T*>(objectPtr));
    return Serializer::serializeToIOBuf(thriftObj);
  }

  // Serialize the object of type T into an IOBuf.
  //
  // @param customSerializeCb  user-defined serialization callback that takes
  //                           T* and returns a unique pointer of IOBuf.
  //                           If nullptr is returned, it will be handled as a
  //                           serialization failure.
  template <typename T>
  std::unique_ptr<folly::IOBuf> serialize(
      std::function<std::unique_ptr<folly::IOBuf>(T*)> customSerializeCb) {
    return customSerializeCb(reinterpret_cast<T*>(objectPtr));
  }

  // cache key of the object to be serialized
  Key key;

  // pointer of the object to be serialized
  uintptr_t objectPtr;
};

// Data structure to be passed in ObjectCache::DeserializeCb.
template <typename ObjectCache>
struct ObjectDeserializer {
  using Key = typename ObjectCache::Key;
  explicit ObjectDeserializer(Key key,
                              folly::StringPiece payload,
                              int objectSize,
                              int ttlSecs,
                              ObjectCache& objCache)
      : key(key),
        payload(payload),
        objectSize(objectSize),
        ttlSecs(ttlSecs),
        objCache_(objCache) {}

  // Deserialize the payload into an object of type T; and insert the object
  // into the cache.
  // Return true if successfully inserted to the cache; false otherwise.
  // T must be a Thrift object.
  template <typename T>
  bool deserialize() {
    Deserializer deserializer{reinterpret_cast<const uint8_t*>(payload.begin()),
                              reinterpret_cast<const uint8_t*>(payload.end())};
    auto ptr = std::make_unique<T>(deserializer.deserialize<T>());
    auto res =
        objCache_.insertOrReplace(key, std::move(ptr), objectSize, ttlSecs);
    return std::get<0>(res) == ObjectCache::AllocStatus::kSuccess;
  }

  // Deserialize the payload into an object of type T; and insert the object
  // into the cache.
  // Return true if successfully inserted to the cache; false otherwise.
  // T is a non-Thrift object.
  //
  // @param fromThriftCb  callback converting a Thrift object to a non-Thrift
  //                      object
  template <typename T, typename ThriftT>
  bool deserialize(std::function<T(ThriftT)> fromThriftCb) {
    Deserializer deserializer{reinterpret_cast<const uint8_t*>(payload.begin()),
                              reinterpret_cast<const uint8_t*>(payload.end())};
    auto thriftObj = deserializer.deserialize<ThriftT>();
    auto ptr = std::make_unique<T>(fromThriftCb(thriftObj));
    auto res =
        objCache_.insertOrReplace(key, std::move(ptr), objectSize, ttlSecs);
    return std::get<0>(res) == ObjectCache::AllocStatus::kSuccess;
  }

  // Deserialize the payload into an object of type T; and insert the object
  // into the cache.
  // Return true if successfully inserted to the cache; false otherwise.
  // T is a non-Thrift object.
  //
  // @param customDeserializeCb  user-defined deserialization callback that
  //                             takes the serialized value of T and returns a
  //                             unique pointer of T. User needs to ensure
  //                             returned std::unique_ptr<T> is not nullptr.
  template <typename T>
  bool deserialize(std::function<std::unique_ptr<T>(folly::StringPiece payload)>
                       customDeserializeCb) {
    auto res = objCache_.insertOrReplace(
        key, std::move(customDeserializeCb(payload)), objectSize, ttlSecs);
    return std::get<0>(res) == ObjectCache::AllocStatus::kSuccess;
  }

  // cache key of the object to be deserialized
  Key key;

  // serialized value of the object
  folly::StringPiece payload;

  // size of the deserialized object
  size_t objectSize;

  // TTL in seconds of the object to be deserialized
  uint32_t ttlSecs;

 private:
  ObjectCache& objCache_;
};

} // namespace objcache2
} // namespace cachelib
} // namespace facebook

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

#include <folly/logging/xlog.h>

#include <atomic>

#include "cachelib/allocator/serialize/gen-cpp2/objects_types.h"

namespace facebook {
namespace cachelib {

// Compact cache metadata that is used to ensure that same type of compact cache
// is attatched to the allocator
//
// When first attaching to allocator, keep a record of the size of key and value
//
// When we save the state of the allocator, we also save this metadata
//
// When compact cache allocator is restored from warmroll, we restore the info
// about size of key and value.
//
// When compact cache is attaching to the allocator, we make sure size of key
// and value are the same
class CCacheMetadata {
 public:
  using SerializationType = serialization::CompactCacheMetadataObject;

  CCacheMetadata() : keySize_(0), valueSize_(0) {}

  CCacheMetadata(const SerializationType& object)
      : keySize_(*object.keySize()), valueSize_(*object.valueSize()) {}

  template <typename CCacheT>
  void initializeOrVerify() {
    const size_t keySize = sizeof(typename CCacheT::Key);
    const size_t valueSize = CCacheT::ValueDescriptor::getSize();
    if (keySize_ != 0) {
      if (keySize_ != keySize) {
        throw std::invalid_argument("size of key mismatch");
      }
      if (valueSize_ != valueSize) {
        throw std::invalid_argument("size of value mismatch");
      }
    } else {
      keySize_ = keySize;
      valueSize_ = valueSize;
    }
  }

  SerializationType saveState() {
    SerializationType object;
    *object.keySize() = keySize_;
    *object.valueSize() = valueSize_;
    return object;
  }

 private:
  size_t keySize_;
  size_t valueSize_;
};

// This is the base call for all of the compact cache allocators in order to
// enforce that only one compact cache can be attached to the same allocator.
// Allocator here means a cache pool in CacheLib. Each compact cache must be
// associated with exactly one cache pool.
class CCacheAllocatorBase {
 public:
  CCacheAllocatorBase() = default;

  CCacheAllocatorBase(const CCacheMetadata::SerializationType& object)
      : ccType_(object) {}

  virtual ~CCacheAllocatorBase() {
    if (isAttached()) {
      XLOG(ERR) << "Current allocator is still attached";
      XDCHECK(false);
    }
  }

  // Initialize and attach the allocator. Note this allocator can only be
  // attached once.
  template <typename CCacheT>
  void attach(CCacheT* ccache) {
    std::lock_guard<std::mutex> lock(resizeLock_);
    ccType_.initializeOrVerify<CCacheT>();
    if (isAttached()) {
      throw std::logic_error("Current allocator is already attached");
    }
    compactCacheResizeFn_ = [ccache]() { ccache->resize(); };
  }

  // Detaching the allocator. Only after detach, can another compact cache
  // instance attach this allocator
  void detach() {
    std::lock_guard<std::mutex> lock(resizeLock_);
    if (!isAttached()) {
      throw std::logic_error("Current allocator is already detached");
    }
    compactCacheResizeFn_ = nullptr;
  }

  bool isAttached() const { return compactCacheResizeFn_ ? true : false; }

  // Resize the compact cache to its new, desired size
  void resizeCompactCache() {
    std::lock_guard<std::mutex> lock(resizeLock_);
    try {
      if (isAttached()) {
        compactCacheResizeFn_();
      } else if (getConfiguredSize() == 0) {
        // this compact cache was disabled, release all of the slabs
        resize();
      }
    } catch (const std::exception& e) {
      XLOG(ERR) << e.what();
    }
  }

  // resize the allocator to configured size
  virtual size_t resize() = 0;

  // return the configured size of the allocator
  virtual size_t getConfiguredSize() const = 0;

 private:
  // Call back function to resize a compact cache when attached
  // This also serves as an indicator that the allocator is attached to a
  // compact cache
  std::function<void()> compactCacheResizeFn_;

 protected:
  std::mutex resizeLock_;

  // meta data of attached compact cache
  CCacheMetadata ccType_;
};

} // namespace cachelib
} // namespace facebook

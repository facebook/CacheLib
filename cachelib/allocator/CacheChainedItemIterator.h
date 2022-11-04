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

#include <folly/io/IOBuf.h>

#include <stdexcept>

#include "cachelib/common/Iterators.h"

namespace facebook {
namespace cachelib {

namespace tests {
template <typename AllocatorT>
class BaseAllocatorTest;
} // namespace tests

// Class to iterate through chained items in the special case that the caller
// has the item but no itemhandle (e.g. during release)
template <typename Cache, typename ItemT>
class CacheChainedItemIterator
    : public detail::IteratorFacade<CacheChainedItemIterator<Cache, ItemT>,
                                    ItemT,
                                    std::forward_iterator_tag> {
 public:
  using Item = ItemT;
  CacheChainedItemIterator() = default;

  Item& dereference() const {
    if (curr_) {
      return *curr_;
    }
    if (curIOBuf_) {
      return *reinterpret_cast<Item*>(curIOBuf_->writableData());
    }
    throw std::runtime_error("no item to dereference");
  }

  // advance the iterator.
  // Do nothing if uninitizliaed.
  void increment() {
    if (curr_) {
      curr_ = curr_->asChainedItem().getNext(*compressor_);
    }
    if (curIOBuf_) {
      curIOBuf_ = curIOBuf_->next();
    }
  }

  bool equal(const CacheChainedItemIterator<Cache, Item>& other) const {
    if (curr_ || other.curr_) {
      return curr_ == other.curr_;
    }
    return curIOBuf_ == other.curIOBuf_;
  }

 private:
  using PtrCompressor = typename Item::PtrCompressor;
  // Private because only CacheT can create this.
  // @param item         Pointer to chained item (nullptr for null iterator)
  // @param compressor   Compressor used to get pointer to next in chain
  explicit CacheChainedItemIterator(Item* item, const PtrCompressor& compressor)
      : curr_(item), compressor_(&compressor) {
    // If @item is not nullptr, check that it is a chained item
    if (curr_ && !curr_->isChainedItem()) {
      throw std::invalid_argument(
          "Cannot initialize ChainedAllocIterator, Item is not a ChainedItem");
    }
  }

  // only NvmCacheT can create with this constructor
  // this is used to construct chained item for ItemDestructor
  // with DipperItem on Navy, Item is allocated at heap (as IOBuf)
  // instead of in allocator memory pool.
  explicit CacheChainedItemIterator(folly::IOBuf* iobuf) : curIOBuf_(iobuf) {
    // If @item is not nullptr, check that it is a chained item or parent item
    // sine IOBuf chains is a circle, so we need to let the parent be the end
    // iterator
    if (curIOBuf_ && !dereference().isChainedItem() &&
        !dereference().hasChainedItem()) {
      throw std::invalid_argument(
          "Cannot initialize ChainedAllocIterator, Item is not a ChainedItem");
    }
  }

  // Current iterator position in chain
  Item* curr_{nullptr};

  // Removed/evicted from NVM
  folly::IOBuf* curIOBuf_{nullptr};

  // Pointer compressor to traverse the chain.
  const PtrCompressor* compressor_{nullptr};

  friend Cache;
  friend typename Cache::NvmCacheT;
  friend typename Cache::ChainedAllocs;
  friend typename Cache::WritableChainedAllocs;

  // For testing
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::BaseAllocatorTest;
};
} // namespace cachelib
} // namespace facebook

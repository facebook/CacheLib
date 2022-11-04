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
#include <stdexcept>
namespace facebook {
namespace cachelib {

// exposes the parent and its chain of allocations through an iterator and
// index. The chain is traversed in the LIFO order. The caller needs to ensure
// that there are no concurrent addChainedItem or popChainedItem while this
// happens.
template <typename Cache, typename Handle, typename Iter>
class CacheChainedAllocs {
 public:
  using Item = typename Cache::Item;
  using ChainedItem = typename Iter::Item;

  CacheChainedAllocs(CacheChainedAllocs&&) = default;
  CacheChainedAllocs& operator=(CacheChainedAllocs&&) = default;

  // return the parent of the chain.
  const Item& getParentItem() const noexcept { return *parent_; }
  // iterate and compute the length of the chain. This is O(N) computation.
  //
  // @return the length of the chain
  size_t computeChainLength() const {
    const auto chain = getChain();
    return std::distance(chain.begin(), chain.end());
  }

  // return the nTh in the chain from the beginning. n = 0 is the first in the
  // chain and last inserted.
  ChainedItem* getNthInChain(size_t n) {
    size_t i = 0;
    for (auto& c : getChain()) {
      if (i++ == n) {
        return &c;
      }
    }
    return nullptr;
  }

  folly::Range<Iter> getChain() const {
    return folly::Range<Iter>{Iter{&head_, compressor_}, Iter{}};
  }

 private:
  friend Cache;
  using LockType = typename Cache::ChainedItemLock;
  using ReadLockHolder = typename LockType::ReadLockHolder;
  using PtrCompressor = typename Item::PtrCompressor;

  CacheChainedAllocs(const CacheChainedAllocs&) = delete;
  CacheChainedAllocs& operator=(const CacheChainedAllocs&) = delete;

  // only the cache can create this view of chained allocs
  //
  // @param l       the lock to be held while iterating on the chain
  // @param parent  handle to the parent
  // @param head    beginning of the chain of the allocations
  // @param c       pointer compressor to traverse the chain
  CacheChainedAllocs(ReadLockHolder l,
                     Handle parent,
                     Item& head,
                     const PtrCompressor& c)
      : lock_(std::move(l)),
        parent_(std::move(parent)),
        head_(head),
        compressor_(c) {
    if (!parent_ || !parent_->hasChainedItem()) {
      throw std::invalid_argument("Parent does not have a chain");
    }

    if (!head_.isChainedItem()) {
      throw std::invalid_argument("Head of chained allocation is invalid");
    }
  }

  // lock protecting the traversal of the chain
  ReadLockHolder lock_;

  // handle to the parent item. holding this ensures that remaining of the
  // chain is not evicted.
  Handle parent_;

  // verify this would not cause issues with the moving slab release logic.
  // Evicting logic is fine since it looks for the parent's refcount
  Item& head_;

  // pointer compressor to traverse the chain.
  const PtrCompressor& compressor_;
};
} // namespace cachelib
} // namespace facebook

#pragma once

#include <stdexcept>

#include "cachelib/common/Iterators.h"

namespace facebook {
namespace cachelib {

namespace tests {
template <typename AllocatorT>
class BaseAllocatorTest;
}

// Class to iterate through chained items in the special case that the caller
// has the item but no itemhandle (e.g. during release)
template <typename Cache>
class CacheChainedItemIterator
    : public detail::IteratorFacade<CacheChainedItemIterator<Cache>,
                                    typename Cache::Item,
                                    std::forward_iterator_tag> {
 public:
  using Item = typename Cache::Item;

  CacheChainedItemIterator() = default;

  Item& dereference() const { return *curr_; }

  void increment() {
    if (curr_) {
      curr_ = curr_->asChainedItem().getNext(*compressor_);
    }
  }

  bool equal(const CacheChainedItemIterator<Cache>& other) const {
    return curr_ == other.curr_;
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

  // Current iterator position in chain
  Item* curr_{nullptr};

  // Pointer compressor to traverse the chain.
  const PtrCompressor* compressor_{nullptr};

  friend Cache;
  friend typename Cache::ChainedAllocs;

  // For testing
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::BaseAllocatorTest;
};
} // namespace cachelib
} // namespace facebook

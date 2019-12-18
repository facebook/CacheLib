#pragma once

#include <boost/iterator/iterator_facade.hpp>
#include <stdexcept>

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
    : public boost::iterator_facade<CacheChainedItemIterator<Cache>,
                                    typename Cache::Item,
                                    boost::forward_traversal_tag> {
 public:
  CacheChainedItemIterator() = default;

 private:
  using Item = typename Cache::Item;
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

  // derefernce the current iterator
  Item& dereference() const { return *curr_; }

  void increment() {
    if (curr_) {
      curr_ = curr_->asChainedItem().getNext(*compressor_);
    }
  }

  bool equal(const CacheChainedItemIterator<Cache>& other) const {
    return curr_ == other.curr_;
  }

  // Current iterator position in chain
  Item* curr_{nullptr};

  // Pointer compressor to traverse the chain.
  const PtrCompressor* compressor_{nullptr};

  friend Cache;
  friend typename Cache::ChainedAllocs;
  friend class boost::iterator_core_access;

  // For testing
  template <typename AllocatorT>
  friend class facebook::cachelib::tests::BaseAllocatorTest;
};
} // namespace cachelib
} // namespace facebook

#pragma once

#include "cachelib/allocator/CacheChainedItemIterator.h"
#include "cachelib/datatype/Buffer.h"
#include "cachelib/datatype/Map.h"

namespace facebook {
namespace cachelib {

// MapView is a read-only version for cachelib Map data structure.
// User can use a cachelib item and its associated chained items to
// create a MapView.
//
// Please note:
// 1. The caller needs to ensure the lifetime of the passed in item& and chained
//    items.
// 2. We do not guarantee "MapView" is synced with "Map",i.e. a
//    MapView is only valid when the corresponding Map is not mutated. The user
//    is responsible for creating a new view if such mutation occurs.
template <typename K, typename V, typename C>
class MapView {
 public:
  using EntryKey = K;
  using EntryValue = V;
  using CacheType = C;

  using Item = typename CacheType::Item;
  using ChainedItemIter = CacheChainedItemIterator<CacheType>;
  using Map = Map<K, V, C>;
  using EntryKeyValue = typename Map::EntryKeyValue;

  // Constructor
  MapView() = default;
  MapView(const Item& parent, const folly::Range<ChainedItemIter>& children);

  // Moving or copying is disallowed
  MapView(MapView&& other) = delete;
  MapView& operator=(MapView&& other) = delete;
  MapView(const MapView& other) = delete;
  MapView& operator=(const MapView& other) = delete;

  // Find a value given the key. Return nullptr if not found
  // @param key   key to an entry in this map
  const EntryValue* find(const EntryKey& key) const;

  // Return number of bytes this map is using for hash table and the buffers
  // This doesn't include cachelib item overhead
  size_t sizeInBytes() const;

  // Return number of elements in this map
  uint32_t size() const;

  using BufferAddr = detail::BufferAddr;
  using Buffer = detail::Buffer;
  class Iterator : public detail::IteratorFacade<Iterator,
                                                 const EntryKeyValue,
                                                 std::forward_iterator_tag> {
   public:
    Iterator() = default;
    explicit Iterator(std::vector<const Buffer*>& buffers)
        : buffers_(&buffers),
          curr_(const_cast<Buffer*>(buffers_->at(index_))->begin()) {
      if (curr_ == Buffer::Iterator()) {
        // Currently, curr_ is invalid. So we increment to try to find
        // an valid iterator
        incrementIntoNextBuffer();
      }
    }

    enum EndT { End };
    Iterator(std::vector<const Buffer*>& buffers, EndT)
        : buffers_(&buffers), index_(buffers_->size()) {}

    detail::BufferAddr getAsBufferAddr() const {
      return BufferAddr{buffers_->size() - index_ - 1 /* itemOffset */,
                        curr_.getDataOffset()};
    }

    // Calling increment when we have reached the end will result in
    // a null iterator.
    // @throw std::out_of_range if we move past the end
    void increment() {
      if (curr_ == Buffer::Iterator{}) {
        throw std::out_of_range(fmt::format(
            "Moving past the end of all buffers. Size of buffers: {}",
            buffers_->size()));
      }

      ++curr_;
      if (curr_ == Buffer::Iterator{}) {
        incrementIntoNextBuffer();
      }
    }

    const EntryKeyValue& dereference() const {
      if (curr_ == Buffer::Iterator{}) {
        throw std::runtime_error(
            "MapView::Iterator:: deferencing a null Iterator.");
      }
      return reinterpret_cast<const EntryKeyValue&>(curr_.dereference());
    }

    bool equal(const Iterator& other) const {
      return index_ == other.index_ && buffers_ == other.buffers_ &&
             curr_ == other.curr_;
    }

   private:
    void incrementIntoNextBuffer() {
      while (curr_ == Buffer::Iterator{}) {
        if (++index_ == buffers_->size()) {
          // we've reached the end of Buffer
          return;
        }
        curr_ = const_cast<Buffer*>(buffers_->at(index_))->begin();
      }
    }

    std::vector<const Buffer*>* buffers_{};
    uint32_t index_{0};
    detail::Buffer::Iterator curr_{};
  };

  // These iterators are only valid when this MapView object is valid
  Iterator begin() { return Iterator{buffers_}; }
  Iterator end() { return Iterator{buffers_, Iterator::End}; }

 private:
  using HashTable = detail::HashTable<EntryKey>;

  // Get the keyValuEntry stored at the corresponding itemOffset and byteOffset
  // @throw std::invalid_argument on addr being nullptr
  const EntryKeyValue* get(BufferAddr addr) const;

  // converted from the parent item, which is a hashtable<key, BufferAddr>
  const HashTable* hashtable_;
  // converted from chained items storing the actual data
  std::vector<const Buffer*> buffers_;
  size_t numBytes_{0};
};

} // namespace cachelib
} // namespace facebook

#include "cachelib/datatype/MapView-inl.h"

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

#include "cachelib/allocator/CacheChainedItemIterator.h"
#include "cachelib/datatype/Buffer.h"
#include "cachelib/datatype/Map.h"

namespace facebook {
namespace cachelib {

// MapView is a read-only version for cachelib Map data structure.
// User can use a cachelib item and its associated chained items to
// create a MapView.
//
// Please note: MapView does NOT own the underlying data
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
  using ChainedItemIter = typename CacheType::ChainedItemIter;
  using Map = Map<K, V, C>;
  using EntryKeyValue = typename Map::EntryKeyValue;

  // Constructor
  MapView() = default;
  MapView(const Item& parent, const folly::Range<ChainedItemIter>& children);

  // Move constructor
  MapView(MapView&& other) noexcept;
  MapView& operator=(MapView&& other) noexcept;

  // Copying is disallowed
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
    explicit Iterator(const std::vector<const Buffer*>& buffers)
        : buffers_(&buffers),
          curr_(const_cast<Buffer*>(buffers_->at(index_))->begin()) {
      if (curr_ == Buffer::Iterator()) {
        // Currently, curr_ is invalid. So we increment to try to find
        // an valid iterator
        incrementIntoNextBuffer();
      }
    }

    enum EndT { End };
    Iterator(const std::vector<const Buffer*>& buffers, EndT)
        : buffers_(&buffers), index_(buffers_->size()) {}

    BufferAddr getAsBufferAddr() const {
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

    const std::vector<const Buffer*>* buffers_{};
    uint32_t index_{0};
    Buffer::Iterator curr_{};
  };

  // These iterators are only valid when this MapView object is valid
  Iterator begin() const { return Iterator{buffers_}; }
  Iterator end() const { return Iterator{buffers_, Iterator::End}; }

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

// ReadOnlyMap is the derived class of MapView with the same read-only
// functionalities (e.g. lookup, iteration).
// Different from MapView, ReadOnlyMap DOES own the underlying data because it
// contains a ReadHandle to hold the ownership.
template <typename K, typename V, typename C>
class ReadOnlyMap : public MapView<K, V, C> {
 public:
  using EntryKey = K;
  using EntryValue = V;
  using CacheType = C;

  using Item = typename CacheType::Item;
  using ReadHandle = typename Item::ReadHandle;
  using MapView = MapView<K, V, C>;

  // Convert a read handle to a cachelib::ReadOnlyMap
  // @param cache   cache allocator to allocate from
  // @param handle  parent handle for this cachelib::ReadOnlyMap
  // @return cachelib::ReadOnlyMap
  static ReadOnlyMap fromReadHandle(CacheType& cache, ReadHandle handle);

  // Constructs null cachelib read-only map
  ReadOnlyMap() = default;
  /* implicit */ ReadOnlyMap(std::nullptr_t) : ReadOnlyMap() {}

  // Move constructor
  ReadOnlyMap(ReadOnlyMap&& other) noexcept;
  ReadOnlyMap& operator=(ReadOnlyMap&& other) noexcept;

  // Copy is disallowed
  ReadOnlyMap(const ReadOnlyMap& other) = delete;
  ReadOnlyMap& operator=(const ReadOnlyMap& other) = delete;

  bool isNullReadHandle() const { return handle_ == nullptr; }

  ReadHandle& viewReadHandle() const { return handle_; }

 private:
  ReadOnlyMap(CacheType& cache, ReadHandle handle);

 private:
  ReadHandle handle_;
};

} // namespace cachelib
} // namespace facebook

#include "cachelib/datatype/MapView-inl.h"

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

namespace facebook {
namespace cachelib {
template <typename K, typename V, typename C>
MapView<K, V, C>::MapView(const Item& parent,
                          const folly::Range<ChainedItemIter>& children) {
  hashtable_ = reinterpret_cast<const HashTable*>(parent.getMemory());
  numBytes_ += parent.getSize();
  for (auto& item : children) {
    numBytes_ += item.getSize();
    buffers_.push_back(reinterpret_cast<const Buffer*>(item.getMemory()));
  }
  // Copy in reverse order since then the index into the vector will line up
  // with our chained item indices given out in BufferAddr
  std::reverse(buffers_.begin(), buffers_.end());
}

template <typename K, typename V, typename C>
MapView<K, V, C>::MapView(MapView&& other) noexcept
    : hashtable_(other.hashtable_),
      buffers_(std::move(other.buffers_)),
      numBytes_(other.numBytes_) {}

template <typename K, typename V, typename C>
MapView<K, V, C>& MapView<K, V, C>::operator=(MapView&& other) noexcept {
  if (this != &other) {
    this->~MapView();
    new (this) MapView(std::move(other));
  }
  return *this;
}

template <typename K, typename V, typename C>
size_t MapView<K, V, C>::sizeInBytes() const {
  return numBytes_;
}

template <typename K, typename V, typename C>
uint32_t MapView<K, V, C>::size() const {
  return hashtable_->numEntries();
}

template <typename K, typename V, typename C>
const typename MapView<K, V, C>::EntryValue* MapView<K, V, C>::find(
    const EntryKey& key) const {
  auto* entry = hashtable_->find(key);
  if (!entry) {
    return nullptr;
  }
  return &get(entry->addr)->value;
}

template <typename K, typename V, typename C>
const typename MapView<K, V, C>::EntryKeyValue* MapView<K, V, C>::get(
    BufferAddr addr) const {
  if (!addr) {
    throw std::invalid_argument("cannot get null address");
  }

  const uint32_t itemOffset = addr.getItemOffset();
  const uint32_t byteOffset = addr.getByteOffset();

  auto* buffer = buffers_.at(itemOffset);
  return reinterpret_cast<const EntryKeyValue*>(buffer->getData(byteOffset));
}

template <typename K, typename V, typename C>
ReadOnlyMap<K, V, C>::ReadOnlyMap(ReadOnlyMap&& other) noexcept
    : MapView(std::move(other)), handle_(std::move(other.handle_)) {}

template <typename K, typename V, typename C>
ReadOnlyMap<K, V, C>& ReadOnlyMap<K, V, C>::operator=(
    ReadOnlyMap&& other) noexcept {
  if (this != &other) {
    this->~ReadOnlyMap();
    new (this) ReadOnlyMap(std::move(other));
  }
  return *this;
}

template <typename K, typename V, typename C>
ReadOnlyMap<K, V, C> ReadOnlyMap<K, V, C>::fromReadHandle(CacheType& cache,
                                                          ReadHandle handle) {
  if (!handle) {
    return {nullptr};
  }
  return ReadOnlyMap(cache, std::move(handle));
}

template <typename K, typename V, typename C>
ReadOnlyMap<K, V, C>::ReadOnlyMap(CacheType& cache, ReadHandle handle)
    : MapView(*handle, cache.viewAsChainedAllocsRange(*handle)),
      handle_(std::move(handle)) {}
} // namespace cachelib
} // namespace facebook

#pragma once

namespace facebook {
namespace cachelib {
template <typename K, typename V, typename C>
MapView<K, V, C>::MapView(const Item& parent,
                          const folly::Range<ChainedItemIter>& children) {
  hashtable_ = reinterpret_cast<HashTable*>(parent.getMemory());
  numBytes_ += parent.getSize();
  for (auto& item : children) {
    numBytes_ += item.getSize();
    buffers_.push_back(reinterpret_cast<Buffer*>(item.getMemory()));
  }
  // Copy in reverse order since then the index into the vector will line up
  // with our chained item indices given out in BufferAddr
  std::reverse(buffers_.begin(), buffers_.end());
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
} // namespace cachelib
} // namespace facebook

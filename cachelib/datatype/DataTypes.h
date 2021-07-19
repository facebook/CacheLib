#pragma once

#include <cstdint>
#include <type_traits>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop

#include "cachelib/allocator/memory/Slab.h"
#include "cachelib/common/Iterators.h"

namespace facebook {
namespace cachelib {
namespace util {
template <typename T>
class ForwardIterator
    : public facebook::cachelib::detail::
          IteratorFacade<ForwardIterator<T>, T, std::forward_iterator_tag> {
 public:
  ForwardIterator() = default;
  ForwardIterator(T* element, T* end) : element_(element), end_(end) {}

  void increment() {
    checkSanity();
    ++element_;
  }

  bool equal(const ForwardIterator& rhs) const {
    return element_ == rhs.element_ && end_ == rhs.end_;
  }

  T& dereference() const { return *element_; }

 private:
  void checkSanity() const {
    if (reinterpret_cast<uintptr_t>(element_) >=
        reinterpret_cast<uintptr_t>(end_)) {
      throw std::out_of_range(folly::sformat(
          "Moving past end pointer. curr: {}, end: {}", element_, end_));
    }
  }

  T* element_{};
  T* end_{};
};

namespace detail {
template <typename...>
using void_t = void;
} // namespace detail

// The following detects whether or not a user-provided value is variable size
// This user-providied type must implement `getStorageSize()` to indicate it
// is variable size.
template <typename, typename = void>
struct is_variable_length : std::false_type {};

template <typename T>
struct is_variable_length<
    T,
    detail::void_t<decltype(std::declval<T>().getStorageSize())>>
    : std::true_type {};

// The following will get value size by using sizeof() for types that do not
// implement `getStorageSize()`
template <typename T,
          typename = std::enable_if_t<!util::is_variable_length<T>::value>>
constexpr uint32_t getValueSize(const T&, int = 0) {
  static_assert(sizeof(T) < Slab::kSize,
                "Value's size must be less than slab size");
  return static_cast<uint32_t>(sizeof(T));
}
template <typename T,
          typename = std::enable_if_t<util::is_variable_length<T>::value>>
uint32_t getValueSize(const T& value, long = 0) {
  return value.getStorageSize();
}
} // namespace util
} // namespace cachelib
} // namespace facebook

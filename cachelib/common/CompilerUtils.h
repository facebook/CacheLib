#pragma once
#include <cstdint>
#define CACHELIB_PACKED_ATTR __attribute__((__packed__))
#define CACHELIB_INLINE __attribute__((__always_inline__))

/**
 * If the value is set to true for some type T, then T is safe to persistently
 * store in shared memory or dipper.
 */
template <class T>
struct IsShmSafe {
  static constexpr bool value = false;
};

namespace detail {
template <size_t>
struct CheckSize {};
} // namespace detail

/**
 * Certifies that the type named by 'Name' is of the given size and is
 * safe to store in shared memory or dipper.
 */
#define CACHELIB_SHM_CERTIFY(Name, size)                                       \
  template <>                                                                  \
  struct IsShmSafe<Name> {                                                     \
    static constexpr bool value = true;                                        \
  };                                                                           \
  constexpr detail::CheckSize<sizeof(Name)> FB_ANONYMOUS_VARIABLE(checkSize) = \
      detail::CheckSize<size>();                                               \
  static_assert(std::is_standard_layout<Name>::value,                          \
                #Name "must be standard layout")

namespace facebook {
namespace cachelib {

// convenience struct for getting the number of bits in a byte.
template <typename T>
struct NumBits {
  static constexpr unsigned int kBitsPerByte = 8;
  static constexpr uint8_t value =
      static_cast<uint8_t>(sizeof(T) * kBitsPerByte);
  static_assert(sizeof(T) * kBitsPerByte <= UINT8_MAX,
                "number of bits in this structure larger than max uint8_t");
};
} // namespace cachelib
} // namespace facebook

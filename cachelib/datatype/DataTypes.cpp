#include "cachelib/datatype/DataTypes.h"
#include "cachelib/allocator/CacheAllocator.h"
#include "cachelib/datatype/FixedSizeArray.h"
#include "cachelib/datatype/Map.h"

// For a new data type, certify it by adding a CL_DATA_CERTIFY(...) in
// check_cachelib_datatype_requirements, at the bottom of this. If the
// data type is a template, fill in the template with some appropriate
// template parameters. See the other data types for some examples.

// For a new required method, do the following
// 1. In the detail namespace near the bottom of this file, define a
//    method checker
// 2. Add a cl_require_method(...) to DataTypeRequirements

#define cl_gen_static_method_check(METHOD)                                   \
  template <typename T, typename Ret, typename... Args>                      \
  static auto test_##METHOD(int)                                             \
      ->::std::is_same<Ret, decltype(T::METHOD(::std::declval<Args...>()))>; \
  cl_check_helper(METHOD)

#define cl_gen_static_method_no_arg_check(METHOD)                             \
  template <typename T, typename Ret>                                         \
  static auto test_##METHOD(int)->::std::is_same<Ret, decltype(T::METHOD())>; \
  cl_check_helper(METHOD)

#define cl_gen_member_method_check(METHOD)                       \
  template <typename T, typename Ret, typename... Args>          \
  static auto test_##METHOD(int)                                 \
      ->::std::is_same<Ret, decltype(::std::declval<T>().METHOD( \
                                ::std::declval<Args...>()))>;    \
  cl_check_helper(METHOD)

#define cl_gen_member_method_no_arg_check(METHOD)                    \
  template <typename T, typename Ret>                                \
  static auto test_##METHOD(int)                                     \
      ->::std::is_same<Ret, decltype(::std::declval<T>().METHOD())>; \
  cl_check_helper(METHOD)

#define cl_check_helper(METHOD)                       \
  template <typename...>                              \
  static auto test_##METHOD(bool)->::std::false_type; \
  template <typename... Args>                         \
  struct has_##METHOD : decltype(test_##METHOD<Args...>(0)) {}

namespace facebook {
namespace cachelib {
namespace detail {

// Define all the functions that a cachelib datatype must have. Use the
// apropriate macros for the function type.
//  cl_gen_static_method_check
//  cl_gen_static_method_no_arg_check
//  cl_gen_member_method_check
//  cl_gen_member_method_no_arg_check
cl_gen_static_method_check(fromItemHandle);
cl_gen_member_method_no_arg_check(resetToItemHandle);
cl_gen_member_method_no_arg_check(viewItemHandle);
cl_gen_member_method_no_arg_check(isNullItemHandle);

template <typename Type>
struct DataTypeRequirements {
// cl_require_method(...)
//   Require Method
//   Return Type
//   [Arguments]
#define cl_require_method(METHOD, ...)                                 \
  static_assert(                                                       \
      ::facebook::cachelib::detail::has_##METHOD<Type, __VA_ARGS__>(), \
      "missing [" #METHOD "]")

  cl_require_method(fromItemHandle, Type, typename Type::ItemHandle);
  cl_require_method(resetToItemHandle, typename Type::ItemHandle);
  cl_require_method(viewItemHandle, const typename Type::ItemHandle&);
  cl_require_method(isNullItemHandle, bool);
};
} // namespace detail

namespace {
#define CL_DATATYPE_CERTIFY(...) \
  ::facebook::cachelib::detail::DataTypeRequirements<__VA_ARGS__>{};
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-value"
#pragma GCC diagnostic ignored "-Wunused-function"
void check_cachelib_datatype_requirements() {
  CL_DATATYPE_CERTIFY(FixedSizeArray<int, LruAllocator>);
}
#pragma GCC diagnostic pop
} // namespace
} // namespace cachelib
} // namespace facebook

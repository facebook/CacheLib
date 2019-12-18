#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>

#include <folly/CppAttributes.h>
#include <folly/Portability.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace details {
template <typename T>
struct NoDeduceType {
  using Type = T;
};
} // namespace details

template <typename T>
using NoDeduce = typename details::NoDeduceType<T>::Type;

// Empty, used to prevent "unused variable"
inline void noop(...) {}

// Convenient function to divide integers as floats. Much better than
// static_cast<double>(a) / (a + b), just fdiv(a, a + b).
inline double fdiv(double a, double b) { return a / b; }

template <typename Class, typename RetType, typename... Args>
inline std::function<RetType(Args...)> bindThis(
    RetType (Class::*memFn)(Args...), Class& self) {
  return [memFn, p = &self](Args... args) {
    return (p->*memFn)(std::forward<Args>(args)...);
  };
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

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

#include <folly/CppAttributes.h>
#include <folly/Portability.h>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <functional>

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

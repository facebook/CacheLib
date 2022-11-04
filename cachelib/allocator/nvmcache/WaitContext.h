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

// abstract interface used for NvmCache to interface with ReadHandle/WriteHandle
// for async API. A wait context indicates an object that is waiting for T to be
// ready.
//
// This is used to maintain state inside NvmCache for every oustanding handle
// that is waiting for a concurrent fill operation.
template <typename T>
struct WaitContext {
  explicit WaitContext() {}
  virtual ~WaitContext() {}

  // interface to notify that the WaitContext is satisfied
  //
  // @param val  the value of the object this context was waiting for.
  virtual void set(T val) = 0;
};

} // namespace cachelib
} // namespace facebook

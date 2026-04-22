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

#include <algorithm>
#include <chrono>
#include <string_view>

namespace facebook::cachelib::trace {
namespace detail {
/**
 * C++20 string literal template parameter hack
 */
template <size_t N>
struct StringParam {
  /* implicit */ constexpr StringParam(const char (&s)[N]) {
    std::copy_n(s, N, s_);
  }
  constexpr std::string_view str() const { return {s_, N}; }
  char s_[N]{};
};
} // namespace detail

/**
 * Profiled<> is a zero-overhead wrapper around a mutex or a baton that allows
 * profiling usage on demand.
 *
 * Simply wrap your type as `trace::Profiled<MutexOrBaton, "name">`, no other
 * changes are required.
 *
 * Profiled mutexes with the same name are considered different "shards" of the
 * same logical mutex.
 * In particular, `std::vector<trace::Profiled<Mutex, "sharded_mutex">>` works
 * as expected and will also automatically collect shard imbalance metrics.
 */
template <typename MutexOrBaton, detail::StringParam name>
class Profiled {
 public:
  /**
   * Note: `explicit` is required to make std::vector<Profiled>>{n} work as
   * expected.
   */
  template <typename... Args>
  explicit Profiled(Args&&... args)
      : mutexOrBaton_(std::forward<Args>(args)...) {}

  ~Profiled() = default;
  Profiled(const Profiled&) = delete;
  Profiled& operator=(const Profiled&) = delete;
  Profiled(Profiled&&) noexcept = delete;
  Profiled& operator=(Profiled&&) noexcept = delete;

  /**
   * Passthrough API to the underlying mutex.
   */
  void lock_shared() { mutexOrBaton_.lock_shared(); }
  void unlock_shared() { mutexOrBaton_.unlock_shared(); }
  bool try_lock() { return mutexOrBaton_.try_lock(); }
  template <typename Rep, typename Period>
  bool try_lock_for(const std::chrono::duration<Rep, Period>& timeout) {
    return mutexOrBaton_.try_lock_for(timeout);
  }
  template <typename Clock, typename Duration>
  bool try_lock_until(
      const std::chrono::time_point<Clock, Duration>& deadline) {
    return mutexOrBaton_.try_lock_until(deadline);
  }
  bool try_lock_shared() { return mutexOrBaton_.try_lock_shared(); }
  template <typename Rep, typename Period>
  bool try_lock_shared_for(const std::chrono::duration<Rep, Period>& timeout) {
    return mutexOrBaton_.try_lock_shared_for(timeout);
  }
  template <typename Clock, typename Duration>
  bool try_lock_shared_until(
      const std::chrono::time_point<Clock, Duration>& deadline) {
    return mutexOrBaton_.try_lock_shared_until(deadline);
  }
  void lock() { mutexOrBaton_.lock(); }
  void unlock() { mutexOrBaton_.unlock(); }

  /**
   * Passthrough API to the underlying baton
   */
  void wait() { mutexOrBaton_.wait(); }
  template <typename Rep, typename Period>
  bool try_wait_for(const std::chrono::duration<Rep, Period>& timeout) {
    return mutexOrBaton_.try_wait_for(timeout);
  }
  void post() { mutexOrBaton_.post(); }
  void reset() { mutexOrBaton_.reset(); }

 private:
  MutexOrBaton mutexOrBaton_;
};

} // namespace facebook::cachelib::trace

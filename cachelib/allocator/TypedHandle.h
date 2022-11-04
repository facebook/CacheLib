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

#include "cachelib/allocator/Handle.h"

namespace facebook {
namespace cachelib {

namespace detail {
template <typename ItemType, typename UserType>
struct DefaultUserTypeConverter {
  UserType& operator()(ItemType& item) {
    return *item.template getMemoryAs<UserType>();
  }

  const UserType& operator()(const ItemType& item) {
    return *item.template getMemoryAs<const UserType>();
  }
};
} // namespace detail

// Converts a WriteHandle to a Typed Handle that will acts as
// a smart pointer for a user defined type.
//
// Example usage:
//  struct MyType {
//    int foo;
//    int bar;
//    char yolo[10];
//  }
//
//  TypedHandleImpl<Item, MyType> typedHandle{
//      cache->find("some key for my type")
//  };
//
// If user wants to include other parts of the item as their structure,
// user needs to supply their own converter class
template <typename T,
          typename U,
          typename Converter = detail::DefaultUserTypeConverter<T, U>>
class TypedHandleImpl {
 public:
  using Item = T;
  using WriteHandle = typename Item::WriteHandle;
  using UserType = U;

  TypedHandleImpl() = default;
  TypedHandleImpl(TypedHandleImpl&&) = default;
  TypedHandleImpl& operator=(TypedHandleImpl&&) = default;

  TypedHandleImpl(const TypedHandleImpl&) = delete;
  TypedHandleImpl& operator=(const TypedHandleImpl&) = delete;

  /* implicit */ TypedHandleImpl(std::nullptr_t) {}
  explicit TypedHandleImpl(WriteHandle handle) : h_(std::move(handle)) {}

  explicit operator bool() const noexcept { return h_.get(); }

  UserType* get() noexcept {
    return h_.get() == nullptr ? nullptr : &(toUserType(*((h_.get()))));
  }

  const UserType* get() const noexcept {
    return h_.get() == nullptr ? nullptr : &(toUserType(*((h_.get()))));
  }

  UserType& operator*() noexcept {
    XDCHECK(get() != nullptr);
    return *get();
  }

  const UserType& operator*() const noexcept {
    XDCHECK(get() != nullptr);
    return *get();
  }

  UserType* operator->() noexcept {
    XDCHECK(get() != nullptr);
    return get();
  }

  const UserType* operator->() const noexcept {
    XDCHECK(get() != nullptr);
    return get();
  }

  const WriteHandle& viewWriteHandle() const { return h_; }
  WriteHandle& viewWriteHandle() { return h_; }

  void reset() { h_.reset(); }

  WriteHandle resetToWriteHandle() && { return WriteHandle{std::move(h_)}; }

 private:
  WriteHandle h_{};

  static UserType& toUserType(Item& it) { return Converter()(it); }
  static const UserType& toUserType(const Item& it) { return Converter()(it); }
};

template <typename T, typename U, typename Converter>
inline bool operator==(const TypedHandleImpl<T, U, Converter>& lhs,
                       const TypedHandleImpl<T, U, Converter>& rhs) {
  return lhs.get() == rhs.get();
}

template <typename T, typename U, typename Converter>
inline bool operator==(const TypedHandleImpl<T, U, Converter>& lhs,
                       std::nullptr_t null) {
  return lhs.get() == null;
}

template <typename T, typename U, typename Converter>
inline bool operator==(std::nullptr_t null,
                       const TypedHandleImpl<T, U, Converter>& rhs) {
  return null == rhs.get();
}

template <typename T, typename U, typename Converter>
inline bool operator!=(const TypedHandleImpl<T, U, Converter>& lhs,
                       const TypedHandleImpl<T, U, Converter>& rhs) {
  return lhs.get() != rhs.get();
}

template <typename T, typename U, typename Converter>
inline bool operator!=(const TypedHandleImpl<T, U, Converter>& lhs,
                       std::nullptr_t null) {
  return lhs.get() != null;
}

template <typename T, typename U, typename Converter>
inline bool operator!=(std::nullptr_t null,
                       const TypedHandleImpl<T, U, Converter>& rhs) {
  return null != rhs.get();
}
} // namespace cachelib
} // namespace facebook

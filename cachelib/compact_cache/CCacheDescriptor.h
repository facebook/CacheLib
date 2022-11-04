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

/**
 * @file ccache_descriptor.h
 * This file defines useful value descriptors to be used with a compact cache.
 *
 * A compact cache maps a key to a value.
 *
 * There can be different configurations depending on the keys and values
 * stored by a particular compact cache instance.
 *
 * 1) Some compact caches encode a part of the key, some don't.
 * 2) Some compact caches store a value along with the key, some don't. In
 *    addition to that, some compact caches provide the possibility to increment
 *    the value if it is a counter.
 *
 * In order to define this behavior, this file defines two class traits: the key
 * descriptors and the value descriptors.
 * The creator of a compact cache must provide the proper descriptors depending
 * on what he wants to achieve.
 *
 * -- Value Descriptors
 *
 * The value descriptor describes the type of a value in the compact cache.
 *
 * There are two value descriptors:
 * - ValueDescriptor<ValueT>
 *       If this value descriptor is used, the compact cache will not provide
 *       the "add" function as there is no semantic reason for being able to
 *       increment this value. This descriptor can be used for
 *       storing everything that is not a counter.
 *       One can use the special type NoValue in order to define a cache that
 *       stores no values: ValueDescriptor<NoValue>.
 * - CounterValueDescriptor<Value, ValueZeroFn, ValuePlusFn>
 *       Can be used to store a value that can be incremented, i.e. a counter.
 *       When using this value descriptor, the compact cache will provide the
 *       add function.
 *       If Value is not an integral type or you want to define a specific
 *       behavior for you counter, you can provide two functors (ValueZeroFn
 *       and ValuePlusFn) for checking if a value is equal to zero and adding
 *       values together. These two functors will default to using the default
 *       == operator, provided that T is an integral type.
 *       When the value reaches 0 (ValueZeroFn::operator() returns true),
 *       the corresponding entry is removed from the cache.
 */

#pragma once

#include <functional>
#include <type_traits>

namespace facebook {
namespace cachelib {

/****************************************************************************/
/* Value descriptors */

namespace detail {

/** Default functor used to check if a value is zero */
template <typename ValueT>
struct EqualToZero {
  constexpr bool operator()(const ValueT& p) const { return p == 0; }
};

} // namespace detail

/**
 * Type requirements of a value descriptor:
 *
 * - Value: a member type that gives the type of the value to be stored in a
 *            bucket entry.
 *
 * - bool isEmpty(const Value&)
 *   Return true if a value is zero.
 *
 * - Value add(const Value&, const Value&)
 *   Return the sum of two values.
 *
 * Note: isEmpty and add can be omitted if the caller never uses the add
 * function of the compact cache.
 */

/**
 * Basic Value descriptor. This value descriptor does not provide isEmpty and
 * add thus any compact cache that uses this descriptor won't implement the
 * "add" operation.
 *
 * @param ValueT type of the value to be stored by the compact cache.
 */
template <typename ValueT>
struct ValueDescriptor {
  constexpr static bool kFixedSize = true;
  using Value = ValueT;
  constexpr static size_t getSize() { return sizeof(Value); }
};

/**
 * Value descriptor for a compact cache that stores values that can be
 * incremented (i.e. counters). A compact cache that uses this descriptor will
 * provide the "add" operation.
 *
 * @param ValueT       type of the value.
 *
 * @param ValueZeroFn Functor to be used for checking if a value is equal to
 *                    zero.
 *                    When a value is equal to zero, the corresponding entry is
 *                    removed from the compact cache.
 *                    This functor is equivalent to comparing the value with 0
 *                    using the == operator by default.
 *
 * @param ValuePlusFn Functor to be used for adding two values together. This
 *                    functor is equivalent to using the + operator by default.
 */
template <typename ValueT,
          typename ValueZeroFn = detail::EqualToZero<ValueT>,
          typename ValuePlusFn = std::plus<ValueT>>
struct CounterValueDescriptor {
  constexpr static bool kFixedSize = true;
  using Value = ValueT;

  constexpr static size_t getSize() { return sizeof(Value); }
  constexpr static bool isEmpty(const Value& val) { return ValueZeroFn()(val); }
  constexpr static Value add(const Value& v1, const Value& v2) {
    return ValuePlusFn()(v1, v2);
  }
};

/**
 * Value descriptor for a compact cache that stores variable sized values.
 *
 * @param MaxSize Maximum value size. The compact cache will not be able to
 *        store values of a size greater than this value. The bucket descriptor
 *        uses this value to define the size of a bucket so that it guarantees
 *        the possibility to contain a value of a size less than or equal to
 *        MaxSize.
 *        However, the user of the compact cache should impose its own
 *        limit on the size of the values he wants to insert. If the limit is
 *        small, you can have more entries in a bucket, and thus a better lru
 *        mechanism (if any). However, if the limit is big, you can put more
 *        objects in the compact cache and save more memory. It is up to the
 *        user to decide what is the right tradeoff here.
 */
template <unsigned MaxSize>
struct VariableSizedValueDescriptor {
  constexpr static bool kFixedSize = false;

  struct Value {
    char data[0];
    // Data is here.
  } __attribute__((__packed__));

  constexpr static unsigned int kMaxSize = MaxSize;

  constexpr static size_t getSize() { return kMaxSize; }
};

/****************************************************************************/
/* Compact Cache descriptor */

/** Special type for a compact cache that stores no value. */
struct CACHELIB_PACKED_ATTR NoValue {
  char value[0];

  // Below are for test purpose only
  NoValue(int /*v*/ = 0) {}
  bool operator==(const NoValue&) const { return true; }
  bool isEmpty() { return true; }
};

/**
 * Define the properties of the data used by a compact cache.
 * This trait aggregates a key type and a value descriptor.
 *
 * @param KeyT             Key type to be used for this compact cache.
 * @param ValueDescriptorT Value descriptor to be used for this compact cache.
 */
template <typename KeyT, typename ValueDescriptorT>
struct CompactCacheDescriptor {
  using Key = KeyT;
  using ValueDescriptor = ValueDescriptorT;

  /** Used to determine if this compact cache stores values */
  constexpr static bool kHasValues =
      !std::is_same<typename ValueDescriptor::Value, NoValue>::value;

  /** Used to determine if this compact cache stores values of a variable
   * size, i.e VariableSizedValueDescriptor is used. */
  constexpr static bool kValuesFixedSize = ValueDescriptor::kFixedSize;
};
} // namespace cachelib
} // namespace facebook

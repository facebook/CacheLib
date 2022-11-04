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

#include <cstddef>
#include <iterator>
#include <type_traits>

/*
 * This is borrowed from folly/detail/Iterators.h which is for folly internal
 * usage only and no guarantee of api compatibility. But it's good enough for
 * cachelib use cases so we keep it here.
 *
 * This contains stripped-down workalikes of some Boost classes:
 *
 *   iterator_adaptor
 *   iterator_facade
 *
 * Rationale: the boost headers providing those classes are surprisingly large.
 * The bloat comes from the headers themselves, but more so, their transitive
 * includes.
 *
 * These implementations are simple and minimal. They may be missing features
 * provided by the Boost classes mentioned above. Also at this time they only
 * support forward-iterators. They provide just enough for the few uses within
 * cachelib; more features will be slapped in here if and when they're needed.
 *
 * These classes may possibly add features as well. Care is taken not to
 * change functionality where it's expected to be the same (e.g. `dereference`
 * will do the same thing).
 *
 * To see how to use these classes, find the instances where this is used within
 * cachelib. Common use cases can also be found in `IteratorsTest.cpp`.
 */

namespace facebook {
namespace cachelib {
namespace detail {

/**
 * Currently this only supports forward and bidirectional iteration.  The
 * derived class must must have definitions for these methods:
 *
 *   void increment();
 *   void decrement(); // optional, to be used with bidirectional
 *   reference dereference() const;
 *   bool equal([appropriate iterator type] const& rhs) const;
 *
 * These names are consistent with those used by the Boost iterator
 * facade / adaptor classes to ease migration classes in this file.
 *
 * Template parameters:
 * D: the deriving class (CRTP)
 * V: value type
 * Tag: the iterator category, one of:
 *   std::forward_iterator_tag
 *   std::bidirectional_iterator_tag
 */
template <class D, class V, class Tag>
class IteratorFacade {
 public:
  using value_type = V;
  using reference = value_type&;
  using pointer = value_type*;
  using difference_type = ssize_t;
  using iterator_category = Tag;

  friend bool operator==(D const& lhs, D const& rhs) { return equal(lhs, rhs); }

  friend bool operator!=(D const& lhs, D const& rhs) { return !(lhs == rhs); }

  /*
   * Allow for comparisons between this and an iterator of some other class.
   * (e.g. a const_iterator version of this, the probable use case).
   * Does a conversion of D (or D reference) to D2, if one exists (otherwise
   * this is disabled).  Disabled if D and D2 are the same, to disambiguate
   * this and the `operator==(D const&) const` method above.
   */

  template <class D2>
  typename std::enable_if<std::is_convertible<D, D2>::value, bool>::type
  operator==(D2 const& rhs) const {
    return D2(asDerivedConst()) == rhs;
  }

  template <class D2>
  bool operator!=(D2 const& rhs) const {
    return !operator==(rhs);
  }

  V& operator*() const { return asDerivedConst().dereference(); }

  V* operator->() const { return std::addressof(operator*()); }

  D& operator++() {
    asDerived().increment();
    return asDerived();
  }

  D operator++(int) {
    auto ret = asDerived(); // copy
    asDerived().increment();
    return ret;
  }

  template <typename T = Tag>
  typename std::enable_if<
      std::is_same<T, std::bidirectional_iterator_tag>::value,
      D&>::type
  operator--() {
    asDerived().decrement();
    return asDerived();
  }

  template <typename T = Tag>
  typename std::enable_if<
      std::is_same<T, std::bidirectional_iterator_tag>::value,
      D>::type
  operator--(int) {
    auto ret = asDerived(); // copy
    asDerived().decrement();
    return ret;
  }

 private:
  D& asDerived() { return static_cast<D&>(*this); }

  D const& asDerivedConst() const { return static_cast<D const&>(*this); }

  static bool equal(D const& lhs, D const& rhs) { return lhs.equal(rhs); }
};

} // namespace detail
} // namespace cachelib
} // namespace facebook

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

#include <folly/container/Array.h>
#include <folly/portability/GTest.h>

#include <map>

#include "cachelib/common/Iterators.h"

using namespace facebook::cachelib::detail;

namespace {
struct IntArrayIterator
    : IteratorFacade<IntArrayIterator, int const, std::forward_iterator_tag> {
  explicit IntArrayIterator(int const* a) : a_(a) {}
  void increment() { ++a_; }
  int const& dereference() const { return *a_; }
  bool equal(IntArrayIterator const& rhs) const { return rhs.a_ == a_; }
  int const* a_;
};
} // namespace

TEST(IteratorsTest, IterFacadeHasCorrectTraits) {
  using TR = std::iterator_traits<IntArrayIterator>;
  static_assert(std::is_same<TR::value_type, int const>::value, "");
  static_assert(std::is_same<TR::reference, int const&>::value, "");
  static_assert(std::is_same<TR::pointer, int const*>::value, "");
  static_assert(
      std::is_same<TR::iterator_category, std::forward_iterator_tag>::value,
      "");
  static_assert(std::is_same<TR::difference_type, ssize_t>::value, "");
}

TEST(IteratorsTest, SimpleIteratorFacade) {
  static const auto kArray = folly::make_array(42, 43, 44);
  IntArrayIterator end(kArray.data() + kArray.size());
  IntArrayIterator iter(kArray.data());
  EXPECT_NE(iter, end);
  EXPECT_EQ(42, *iter);
  ++iter;
  EXPECT_NE(iter, end);
  EXPECT_EQ(43, *iter);
  ++iter;
  EXPECT_NE(iter, end);
  EXPECT_EQ(44, *iter);
  ++iter;
  EXPECT_EQ(iter, end);
}

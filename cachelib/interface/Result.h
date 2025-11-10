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

#include <folly/Expected.h>

namespace facebook::cachelib::interface {

struct Error {
  enum class Code {
    INVALID_ARGUMENTS,
    INVALID_CONFIG,

    // Allocation errors
    NO_SPACE,

    // Insertion errors
    INSERT_FAILED,
    ALREADY_INSERTED,

    // Remove errors
    REMOVE_FAILED,
  };

  Error(Code code, std::string error) : code_(code), error_(std::move(error)) {}

  Code code_;
  std::string error_;
};

FOLLY_ALWAYS_INLINE folly::Unexpected<Error> makeError(Error::Code code,
                                                       std::string error) {
  return folly::makeUnexpected(Error{code, std::move(error)});
}

template <typename Value>
using Result = folly::Expected<Value, Error>;
using UnitResult = Result<folly::Unit>;

} // namespace facebook::cachelib::interface

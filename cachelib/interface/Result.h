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

#include <magic_enum/magic_enum.hpp>

namespace facebook::cachelib::interface {

struct Error {
  enum class Code : uint8_t {
    // User errors
    INVALID_ARGUMENTS,
    INVALID_CONFIG,

    // Allocation errors
    ALLOCATE_FAILED,
    NO_SPACE,

    // Insertion errors
    INSERT_FAILED,
    ALREADY_INSERTED,
    WRITE_BACK_FAILED,

    // Lookup errors
    FIND_FAILED,

    // Remove errors
    REMOVE_FAILED,

    // Other
    UNIMPLEMENTED,
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

namespace std {
inline ostream& operator<<(ostream& os,
                           const facebook::cachelib::interface::Error& error) {
  os << "Error (" << magic_enum::enum_name(error.code_) << ", code "
     << static_cast<uint8_t>(error.code_) << "): " << error.error_;
  return os;
}
} // namespace std

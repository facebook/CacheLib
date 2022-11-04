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

#include <folly/dynamic.h>
#include <folly/json.h>

#include <string>
#include <unordered_map>
#include <vector>

#define JSONSetVal(configJson, field)             \
  do {                                            \
    const auto* ptr = configJson.get_ptr(#field); \
    if ((ptr)) {                                  \
      setValImpl(field, *ptr);                    \
    }                                             \
  } while (false)

namespace facebook {
namespace cachelib {
namespace cachebench {
struct JSONConfig {
  static void setValImpl(uint32_t& field, const folly::dynamic& val) {
    field = val.asInt();
  }

  static void setValImpl(uint64_t& field, const folly::dynamic& val) {
    field = val.asInt();
  }

  // On some platforms (e.g Mac OS 12) 'size_t' and 'uint64_t' are not the same
  // type: while both are unsigned 64bit integers, size_t is "unsigned long"
  // while "uint64_t" is "unsigned long long". In C they are automatically
  // converted from one to the other. In C++ with templates, they result in
  // different types and require explicit specialization.
  // BUT,
  // On platforms where "size_t" is the same as "uint64_t" (e.g. Linux/amd64),
  // template specializing of "size_t" will cause compilation error (due to a
  // duplicated type). The template code below will safely create the size_t
  // specialization only if it differs from uint64_t. See here:
  // https://techoverflow.net/2019/06/13/stdenable_if-and-stdis_same-minimal-example/
  // and https://eli.thegreenplace.net/2014/sfinae-and-enable_if/
  // TODO: use std::is_integral<T> to merge uint32/uint64/size_t etc.
  template <
      class T = uint64_t,
      typename std::enable_if<std::negation<std::is_same<size_t, T>>::value,
                              void*>::type = nullptr>
  static void setValImpl(size_t& field, const folly::dynamic& val) {
    field = val.asInt();
  }

  static void setValImpl(double& field, const folly::dynamic& val) {
    field = val.asDouble();
  }

  static void setValImpl(bool& field, const folly::dynamic& val) {
    field = val.asBool();
  }

  static void setValImpl(std::string& field, const folly::dynamic& val) {
    field = val.asString();
  }

  template <typename ValType>
  static void setMapValImpl(
      std::unordered_map<uint32_t, std::vector<ValType>>& field,
      const folly::dynamic& key,
      std::vector<ValType>&& vals) {
    field[key.asInt()] = vals;
  }

  template <typename ValType>
  static void setValImpl(std::vector<ValType>& field,
                         const folly::dynamic& val) {
    if (val.isArray()) {
      field.clear();
      for (const auto& v : val) {
        ValType tmp;
        setValImpl(tmp, v);
        field.push_back(tmp);
      }
    } else {
      folly::throw_exception<folly::TypeError>("array", val.type());
    }
  }

  template <typename KeyType, typename ValType>
  static void setValImpl(std::unordered_map<KeyType, ValType>& field,
                         const folly::dynamic& val) {
    if (val.isObject()) {
      field.clear();
      for (const auto& pair : val.items()) {
        KeyType key;
        setValImpl(key, pair.first);
        ValType value;
        setValImpl(value, pair.second);
        field[key] = value;
      }
    } else {
      folly::throw_exception<folly::TypeError>("object", val.type());
    }
  }

  template <typename KeyType, typename ValType>
  static void setValImpl(
      std::unordered_map<KeyType, std::vector<ValType>>& field,
      const folly::dynamic& val) {
    if (val.isObject()) {
      field.clear();
      for (const auto& pair : val.items()) {
        if (pair.second.isArray()) {
          std::vector<ValType> tmp;
          setValImpl(tmp, pair.second);
          setMapValImpl(field, pair.first, std::move(tmp));
        }
      }
    } else {
      folly::throw_exception<folly::TypeError>("object", val.type());
    }
  }

  static void setValImpl(folly::dynamic& field, const folly::dynamic& val) {
    field = val;
  }
};

namespace {
template <size_t s>
struct Options {};
} // namespace

template <typename Type, size_t size>
constexpr void checkCorrectSize() {
#ifndef SKIP_OPTION_SIZE_VERIFY
  Options<sizeof(Type)> var = Options<size>{};
  (void)var;
#endif
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

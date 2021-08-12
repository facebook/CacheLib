/*
 * Copyright (c) Facebook, Inc. and its affiliates.
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
    }
  }
};

namespace {
template <size_t s>
struct Options {};
} // namespace

template <typename Type, size_t size>
constexpr void checkCorrectSize() {
  Options<sizeof(Type)> var = Options<size>{};
  (void)var;
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

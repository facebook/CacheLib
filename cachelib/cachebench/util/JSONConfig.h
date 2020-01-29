#pragma once

#include <string>
#include <vector>

#include <folly/dynamic.h>
#include <folly/json.h>

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

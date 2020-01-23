#pragma once
#include <folly/Format.h>
#include <folly/Random.h>
#include <ctime>
#include <string>
#include <vector>

namespace facebook {
namespace cachelib {
namespace cachebench {

// Operations that the stressor supports
// They translate into the following cachelib operations
//  Set: allocate + insertOrReplace
//  get: find
//  del: remove
enum class OpType {
  kSet = 0,
  kGet,
  kDel,

  kAddChained, // allocate a parent and a certain number of chained items
  // key will be randomly generated, operation will be get
  kLoneGet,
  kLoneSet,

  kSize
};

struct Request {
  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e)
      : key(k), sizeBegin(b), sizeEnd(e) {}

  static std::string getUniqueKey() {
    return std::string(folly::to<std::string>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count() +
        folly::Random::rand32()));
  }

  Request(Request&& r) noexcept
      : key(r.key), sizeBegin(r.sizeBegin), sizeEnd(r.sizeEnd) {}
  Request& operator=(Request&& r) = delete;

  std::string& key;
  // size iterators in case this request is
  // deemed to be a chained item.
  // If not chained, the size is *sizeBegin
  std::vector<size_t>::iterator sizeBegin;
  std::vector<size_t>::iterator sizeEnd;

  std::optional<uint64_t> requestId;
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

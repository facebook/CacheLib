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

enum class OpResultType {
  kNop = 0,
  kGetMiss,
  kGetHit,
  kSetSuccess,
  kSetFailure
};

struct Request {
  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e)
      : key(k), sizeBegin(b), sizeEnd(e) {}

  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e,
          OpType o)
      : key(k), sizeBegin(b), sizeEnd(e), op(o) {}

  Request(std::string& k,
          std::vector<size_t>::iterator b,
          std::vector<size_t>::iterator e,
          OpType o,
          uint32_t ttl,
          uint64_t reqId)
      : key(k),
        sizeBegin(b),
        sizeEnd(e),
        ttlSecs(ttl),
        requestId(reqId),
        op(o) {}

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

  OpType getOp() const noexcept { return op.load(); }
  void setOp(OpType o) noexcept { op = o; }

  std::string& key;
  // size iterators in case this request is
  // deemed to be a chained item.
  // If not chained, the size is *sizeBegin
  std::vector<size_t>::iterator sizeBegin;
  std::vector<size_t>::iterator sizeEnd;

  // TTL in seconds.
  const uint32_t ttlSecs{0};

  const std::optional<uint64_t> requestId;

 private:
  std::atomic<OpType> op{OpType::kGet};
};

} // namespace cachebench
} // namespace cachelib
} // namespace facebook

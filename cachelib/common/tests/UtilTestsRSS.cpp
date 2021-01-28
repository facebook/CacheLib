#include <folly/Random.h>
#include <gtest/gtest.h>
#include <sys/mman.h>

#include <atomic>
#include <thread>

#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace tests {

TEST(Util, MemRSS) {
  auto val = util::getRSSBytes();
  EXPECT_GT(val, 0);
  const size_t len = 16 * 1024 * 1024;
  void* ptr = ::mmap(nullptr, len, PROT_WRITE | PROT_READ,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  EXPECT_NE(MAP_FAILED, ptr);
  SCOPE_EXIT { ::munmap(ptr, len); };
  std::memset(reinterpret_cast<char*>(ptr), 5, len);
  // sleep to let the stat catch up.
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  auto newVal = util::getRSSBytes();
  EXPECT_GT(newVal, val);
  EXPECT_GE(newVal - val, len)
      << folly::sformat("newVal= {}, val = {}, len = {}", newVal, val, len);
}

} // namespace tests
} // namespace cachelib
} // namespace facebook

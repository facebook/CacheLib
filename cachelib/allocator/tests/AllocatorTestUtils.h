#include "cachelib/allocator/RebalanceStrategy.h"

namespace facebook {
namespace cachelib {
namespace tests {
struct AlwaysPickOneRebalanceStrategy : public RebalanceStrategy {
  // Figure out which allocation class has the highest number of allocations
  // and release
 public:
  const ClassId victim;
  const ClassId receiver;
  AlwaysPickOneRebalanceStrategy(ClassId victim, ClassId receiver)
      : RebalanceStrategy(PickNothingOrTest),
        victim(victim),
        receiver(receiver) {}

 private:
  ClassId pickVictim(const CacheBase&, PoolId) { return victim; }

  ClassId pickVictimImpl(const CacheBase& allocator, PoolId pid) override {
    return pickVictim(allocator, pid);
  }

  RebalanceContext pickVictimAndReceiverImpl(const CacheBase& allocator,
                                             PoolId pid) override {
    return {pickVictim(allocator, pid), receiver};
  }
};
} // namespace tests
} // namespace cachelib
} // namespace facebook

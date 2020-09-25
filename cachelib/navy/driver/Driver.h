#pragma once

#include <memory>
#include <stdexcept>
#include <utility>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/AbstractCache.h"
#include "cachelib/navy/admission_policy/AdmissionPolicy.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/engine/Engine.h"
#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {
// Constructor can throw but system remains in the valid state. Caller can
// fix parameters and re-run. If any other member function throws
// (only std::{bad_alloc, out_of_range}), it means system is in a *corrupted*
// state and program should terminate. We let the user to take actions before
// termination.
class Driver final : public AbstractCache {
 public:
  struct Config {
    std::unique_ptr<Device> device;
    std::unique_ptr<JobScheduler> scheduler;
    std::unique_ptr<Engine> largeItemCache;
    std::unique_ptr<Engine> smallItemCache;
    std::unique_ptr<AdmissionPolicy> admissionPolicy;
    uint32_t smallItemMaxSize{};
    // Limited by scheduler parallelism (thread), this is large enough value to
    // mean "no limit".
    uint32_t maxConcurrentInserts{1'000'000};
    uint64_t maxParcelMemory{256 << 20}; // 256MB
    size_t metadataSize{};

    Config& validate();
  };

  explicit Driver(Config&& config);
  Driver(const Driver&) = delete;
  Driver& operator=(const Driver&) = delete;
  ~Driver() override;

  Status insert(BufferView key, BufferView value) override;
  Status insertAsync(BufferView key,
                     BufferView value,
                     InsertCallback cb) override;
  Status lookup(BufferView key, Buffer& value) override;
  Status lookupAsync(BufferView key, LookupCallback cb) override;
  Status remove(BufferView key) override;
  Status removeAsync(BufferView key, RemoveCallback cb) override;
  void flush() override;
  void reset() override;
  void persist() const override;
  bool recover() override;

  uint64_t getSize() const override;
  void getCounters(const CounterVisitor& visitor) const override;

 private:
  struct ValidConfigTag {};

  // Assumes that @config was validated with Config::validate
  Driver(Config&& config, ValidConfigTag);
  void onEviction(BufferView key, uint32_t valueSize);

  // Select engine to insert key/value. Returns a pair:
  //   - first: engine to insert key/value
  //   - second: the other engine to remove key
  std::pair<Engine&, Engine&> select(BufferView key, BufferView value) const;
  void updateLookupStats(Status status) const;
  Status removeHashedKey(HashedKey hk);
  bool admissionTest(HashedKey hk, BufferView value) const;

  const uint32_t smallItemMaxSize_{};
  const uint32_t maxConcurrentInserts_{};
  const uint64_t maxParcelMemory_{};
  const size_t metadataSize_{};

  std::unique_ptr<Device> device_;
  std::unique_ptr<JobScheduler> scheduler_;
  // Large item cache assumed to have fast response in case entry doesn't
  // exists (check metadata only).
  std::unique_ptr<Engine> largeItemCache_;
  // Lookup small item cache only if large item cache has no entry.
  std::unique_ptr<Engine> smallItemCache_;
  std::unique_ptr<AdmissionPolicy> admissionPolicy_;

  mutable AtomicCounter insertCount_;
  mutable AtomicCounter succInsertCount_;
  mutable AtomicCounter lookupCount_;
  mutable AtomicCounter succLookupCount_;
  mutable AtomicCounter removeCount_;
  mutable AtomicCounter succRemoveCount_;
  mutable AtomicCounter rejectedCount_;
  mutable AtomicCounter rejectedConcurrentInsertsCount_;
  mutable AtomicCounter rejectedParcelMemoryCount_;
  mutable AtomicCounter rejectedBytes_;
  mutable AtomicCounter ioErrorCount_;
  mutable AtomicCounter parcelMemory_; // In bytes
  mutable AtomicCounter concurrentInserts_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

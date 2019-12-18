#pragma once
#include "cachelib/cachebench/consistency/ValueHistory.h"

namespace facebook {
namespace cachelib {
namespace cachebench {

class LogEventStream : public EventStream {
 public:
  ~LogEventStream() override = default;

  void event(uint32_t index,
             uint32_t other,
             const char* eventName,
             bool end,
             bool hasValue,
             uint64_t value,
             EventInfo info) override;

  std::string format() const;

 private:
  static constexpr uint32_t kMaxReport{30};

  struct Event {
    const char* eventName{nullptr};
    uint32_t other{};
    bool end{false};
    bool hasValue{false};
    uint64_t value{};
    EventInfo info;
  };

  Event events_[kMaxReport];
  uint32_t count_{};
  bool overflow_{false};
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

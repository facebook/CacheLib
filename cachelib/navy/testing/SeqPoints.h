#pragma once

#include "cachelib/navy/common/CompilerUtils.h"

#include <condition_variable>
#include <cstdint>
#include <map>
#include <mutex>
#include <string>

namespace facebook {
namespace cachelib {
namespace navy {
// When you want to test that one thread T1 reaches a certain point before
// another thread T2, you can use this class.
//
// T1 invokes @reached(<SP id>) on synchronization point (SP) that it has
// reached. Thread T2 waits on that synchronization point: @wait(<SP id>).
// SP ids must start with 0 and be sequential integers: 0, 1, 2, ...
//
// For example, if I want ensure that T1 writes before T2 read, I can do
// something like:
//
//   SeqPoints sp = makeSeqPoints();
//   sp->setName(0, "t1 has finished writing");
//   std::thread t1{[&sq] {
//     write();
//     sp->reached(0);
//   }};
//   std::thread t2{[&sq] {
//     sp->wait(0);
//     read();
//   }};
//   t1.join();
//   t2.join();
//
// The above will print something like:
//  Wait 0 t1 has finished writing
//  Reached 0 t1 has finished writing
//
// When debugging, pass in SeqPoints::defaultLogger()
class SeqPoints {
 public:
  enum class Event {
    Wait,
    Reached,
  };

  using Logger =
      std::function<void(Event event, uint32_t idx, const std::string& name)>;

  static Logger defaultLogger();

  explicit SeqPoints(Logger logger = {}) : logger_(std::move(logger)) {}

  SeqPoints(const SeqPoints&) = delete;
  SeqPoints& operator=(const SeqPoints&) = delete;

  void reached(uint32_t idx);

  // Block until reached() is called on the same index. If reached()
  // had already been invoked on the same index, return right away.
  void wait(uint32_t idx);

  // Waits up to @dur time. Returns false if timed out.
  bool waitFor(uint32_t idx, std::chrono::microseconds dur);

  // Optional, but if called, then a sequence point will be associated
  // with a string description, which is useful for debugging.
  void setName(uint32_t idx, std::string msg);

 private:
  struct Point {
    bool reached{false};
    std::string name{};
  };

  void log(Event event, uint32_t idx, const std::string& name);

  mutable std::mutex mutex_;
  mutable std::condition_variable cv_;
  std::map<uint32_t, Point> points_;
  const Logger logger_;
};
} // namespace navy
} // namespace cachelib
} // namespace facebook

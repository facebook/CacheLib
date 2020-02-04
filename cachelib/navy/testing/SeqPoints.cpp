#include "cachelib/navy/testing/SeqPoints.h"

#include <folly/Range.h>
#include <glog/logging.h>

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
folly::StringPiece toString(SeqPoints::Event event) {
  switch (event) {
  case SeqPoints::Event::Wait:
    return "Wait";
  case SeqPoints::Event::Reached:
    return "Reached";
  }
}
} // namespace

SeqPoints::Logger SeqPoints::defaultLogger() {
  return [](Event event, uint32_t idx, const std::string& name) {
    LOG(ERROR) << toString(event) << " " << idx << " " << name;
  };
}

void SeqPoints::reached(uint32_t idx) {
  std::lock_guard<std::mutex> lock{mutex_};
  log(Event::Reached, idx, points_[idx].name);
  points_[idx].reached = true;
  cv_.notify_all();
}

void SeqPoints::wait(uint32_t idx) {
  std::unique_lock<std::mutex> lock{mutex_};
  log(Event::Wait, idx, points_[idx].name);
  while (!points_[idx].reached) {
    cv_.wait(lock);
  }
}

bool SeqPoints::waitFor(uint32_t idx, std::chrono::microseconds dur) {
  auto until = std::chrono::steady_clock::now() + dur;
  std::unique_lock<std::mutex> lock{mutex_};
  log(Event::Wait, idx, points_[idx].name);
  while (!points_[idx].reached) {
    if (cv_.wait_until(lock, until) == std::cv_status::timeout) {
      return false;
    }
  }
  return true;
}

void SeqPoints::setName(uint32_t idx, std::string msg) {
  std::lock_guard<std::mutex> lock{mutex_};
  points_[idx].name = std::move(msg);
}

void SeqPoints::log(Event event, uint32_t idx, const std::string& name) {
  if (logger_) {
    logger_(event, idx, name);
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

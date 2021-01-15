#pragma once

#include <chrono>
#include <functional>
#include <thread>
#include <vector>

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace detail {

inline std::chrono::seconds executeParallel(
    std::function<void(size_t start, size_t end)> fn,
    size_t numThreads,
    size_t count,
    size_t offset = 0) {
  numThreads = std::max(numThreads, 1UL);
  auto startTime = std::chrono::steady_clock::now();
  const size_t perThread = count / numThreads;
  std::vector<std::thread> processingThreads;
  for (size_t i = 0; i < numThreads; i++) {
    processingThreads.emplace_back([i, perThread, offset, &fn]() {
      size_t blockStart = offset + i * perThread;
      size_t blockEnd = blockStart + perThread;
      fn(blockStart, blockEnd);
    });
  }
  fn(offset + perThread * numThreads, offset + count);
  for (auto& t : processingThreads) {
    t.join();
  }

  return std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - startTime);
}

inline std::chrono::seconds executeParallel(std::function<void()> fn,
                                            size_t numThreads) {
  numThreads = std::max(numThreads, 1UL);
  auto startTime = std::chrono::steady_clock::now();
  std::vector<std::thread> processingThreads;
  for (size_t i = 0; i < numThreads; i++) {
    processingThreads.emplace_back([&fn]() { fn(); });
  }
  for (auto& t : processingThreads) {
    t.join();
  }

  return std::chrono::duration_cast<std::chrono::seconds>(
      std::chrono::steady_clock::now() - startTime);
}
} // namespace detail
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

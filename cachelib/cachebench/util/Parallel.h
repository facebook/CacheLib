#pragma once
#include <chrono>
#include <functional>
#include <thread>
#include <vector>

namespace facebook {
namespace cachelib {
namespace cachebench {
namespace detail {
std::chrono::seconds executeParallel(
    std::function<void(size_t start, size_t end)> fn,
    size_t numThreads,
    size_t count,
    size_t offset= 0);

std::chrono::seconds executeParallel(std::function<void()> fn,
                                     size_t numThreads);
} // namespace detail
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

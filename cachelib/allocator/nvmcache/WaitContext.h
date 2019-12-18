#pragma once

namespace facebook {
namespace cachelib {

// abstract interface used for NvmCache to interface with ItemHandle for async
// API. A wait context indicates an object that is waiting for T to be ready.
//
// This is used to maintain state inside NvmCache for every oustanding handle
// that is waiting for a concurrent fill operation.
template <typename T>
struct WaitContext {
  explicit WaitContext() {}
  virtual ~WaitContext() {}

  // interface to notify that the WaitContext is satisfied
  //
  // @param val  the value of the object this context was waiting for.
  virtual void set(T val) = 0;
};

} // namespace cachelib
} // namespace facebook

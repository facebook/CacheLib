#pragma once

#include <folly/Function.h>
#include <folly/fibers/Baton.h>
#include <folly/futures/Future.h>
#include <folly/logging/xlog.h>

#include <iostream>
#include <mutex>

#include "cachelib/allocator/nvmcache/WaitContext.h"

namespace facebook {
namespace cachelib {
namespace detail {
enum class HandleFlags : uint8_t {
  // Indicates if a handle has been inserted into cache
  kNascent = 1 << 0,

  // Indicate if the item was expired
  kExpired = 1 << 1,

  // Indicate if we went to NvmCache to look for this item
  kWentToNvm = 1 << 2,
};

// RAII class that manages cache item pointer lifetime. These handles
// can only be created by a CacheAllocator and upon destruction the handle
// takes care of releasing the item to the correct cache allocator instance.
// Handle must be destroyed *before* the instance of the CacheAllocator
// gets destroyed.
template <typename T>
struct HandleImpl {
  using Item = T;
  using CacheT = typename T::CacheT;

  HandleImpl() = default;
  HandleImpl(std::nullptr_t) {}

  void reset() noexcept {
    waitContext_.reset();

    if (it_ == nullptr) {
      return;
    }

    assert(alloc_ != nullptr);
    try {
      alloc_->release(it_, isNascent());
    } catch (const std::exception& e) {
      XLOGF(CRITICAL, "Failed to release {:#10x} : {}", static_cast<void*>(it_),
            e.what());
    }
    it_ = nullptr;
  }

  // Waits for item (if async op in progress) and then releases item's
  // ownership to the caller.
  Item* release() noexcept {
    auto ret = getImpl();
    if (waitContext_) {
      waitContext_->releaseHandle();
      waitContext_.reset();
    } else {
      it_ = nullptr;
    }
    return ret;
  }

  ~HandleImpl() noexcept { reset(); }

  HandleImpl(const HandleImpl&) = delete;
  HandleImpl& operator=(const HandleImpl&) = delete;

  FOLLY_ALWAYS_INLINE HandleImpl(HandleImpl&& other) noexcept
      : alloc_(other.alloc_),
        it_(other.releaseItem()),
        waitContext_(std::move(other.waitContext_)),
        flags_(other.getFlags()) {}

  FOLLY_ALWAYS_INLINE HandleImpl& operator=(HandleImpl&& other) noexcept {
    if (this != &other) {
      this->~HandleImpl();
      new (this) HandleImpl(std::move(other));
    }
    return *this;
  }

  // == and != operators for comparison with Item*
  friend bool operator==(const HandleImpl& a, const Item* it) noexcept {
    return a.get() == it;
  }
  friend bool operator==(const Item* it, const HandleImpl& a) noexcept {
    return a == it;
  }
  friend bool operator!=(const HandleImpl& a, const Item* it) noexcept {
    return !(a == it);
  }
  friend bool operator!=(const Item* it, const HandleImpl& a) noexcept {
    return !(a == it);
  }

  // == and != operators for comparison with nullptr
  friend bool operator==(const HandleImpl& a, std::nullptr_t) noexcept {
    return a.get() == nullptr;
  }
  friend bool operator==(std::nullptr_t nullp, const HandleImpl& a) noexcept {
    return a == nullp;
  }
  friend bool operator!=(const HandleImpl& a, std::nullptr_t nullp) noexcept {
    return !(a == nullp);
  }
  friend bool operator!=(std::nullptr_t nullp, const HandleImpl& a) noexcept {
    return !(a == nullp);
  }

  // == and != operator
  friend bool operator==(const HandleImpl& a, const HandleImpl& b) noexcept {
    return a.get() == b.get();
  }
  friend bool operator!=(const HandleImpl& a, const HandleImpl& b) noexcept {
    return !(a == b);
  }

  // for use in bool contexts like `if (handle) { ... }`
  FOLLY_ALWAYS_INLINE explicit operator bool() const noexcept {
    return get() != nullptr;
  }

  // accessors. Calling get on handle with isReady() == false blocks the thread
  // until the handle is ready.
  FOLLY_ALWAYS_INLINE Item* operator->() const noexcept { return get(); }
  FOLLY_ALWAYS_INLINE Item& operator*() const noexcept { return *get(); }
  FOLLY_ALWAYS_INLINE Item* get() const noexcept { return getImpl(); }

  folly::SemiFuture<HandleImpl> toSemiFuture() && {
    if (isReady()) {
      return folly::makeSemiFuture(std::forward<HandleImpl>(*this));
    }
    folly::Promise<HandleImpl> promise;
    auto semiFuture = promise.getSemiFuture();
    auto cb = onReady([p = std::move(promise)](HandleImpl handle) mutable {
      p.setValue(std::move(handle));
    });
    if (cb) {
      // Handle became ready after the initial isReady check. So we will run
      // the callback to set the promise inline.
      cb(std::move(*this));
      return semiFuture;
    } else {
      return std::move(semiFuture).deferValue([](HandleImpl handle) {
        if (handle) {
          // Increment one refcount on user thread since we transferred a handle
          // from a cachelib internal thread.
          handle.alloc_->adjustHandleCountForThread(1);
        }
        return handle;
      });
    }
  }

  using ReadyCallback = folly::Function<void(HandleImpl)>;

  // Return true iff item handle is ready to use.
  // Empty handles are considered ready with it_ == nullptr.
  FOLLY_ALWAYS_INLINE bool isReady() const noexcept {
    return waitContext_ ? waitContext_->isReady() : true;
  }

  // Return true if this item has a wait context which means
  // it has missed in DRAM and went to nvm cache.
  bool wentToNvm() const noexcept {
    return getFlags() & static_cast<uint8_t>(HandleFlags::kWentToNvm);
  }

  // Return true if this handle couldn't be fulfilled because the item had
  // already expired. If item is present then the source of truth
  // lies with the actual item.
  bool wasExpired() const noexcept {
    return getFlags() & static_cast<uint8_t>(HandleFlags::kExpired);
  }

  // blocks until `isReady() == true`.
  void wait() const noexcept {
    if (isReady()) {
      return;
    }
    CHECK(waitContext_.get() != nullptr);
    waitContext_->wait();
  }

  // Set the onReady callback which should be invoked once the item is ready.
  // If the item is ready, the callback is returned back to the user for
  // execution.
  //
  // @param callBack   callback function
  //
  // @return     an empty function if the callback was enqueued to be
  //             executed when the handle becomes ready.
  //             if the handle becomes/is ready, this returns the
  //             original callback back to the caller to execute.
  //
  FOLLY_NODISCARD ReadyCallback onReady(ReadyCallback&& callBack) {
    return (waitContext_) ? waitContext_->onReady(std::move(callBack))
                          : std::move(callBack);
  }

 private:
  struct ItemWaitContext : public WaitContext<HandleImpl> {
    explicit ItemWaitContext(CacheT& alloc) : alloc_(alloc) {}

    // @return      managed item pointer
    // NOTE: get() blocks the thread until isReady is true
    Item* get() const noexcept {
      wait();
      XDCHECK(isReady());
      return it_.load(std::memory_order_acquire);
    }

    // Wait until we have the item
    void wait() const noexcept {
      if (isReady()) {
        return;
      }
      baton_.wait();
      XDCHECK(isReady());
    }

    uint8_t getFlags() const { return flags_; }

    // Assumes ownership of the item managed by hdl
    // and invokes the onReadyCallback_
    // postcondition: `isReady() == true`
    //
    // NOTE: It's a bug to set a hdl that's already ready and can
    // terminate the application. This is only used internally within
    // cachelib and shouldn't be exposed outside of cachelib to applications.
    void set(HandleImpl hdl) override {
      XDCHECK(!isReady());
      SCOPE_EXIT { hdl.release(); };

      flags_ = hdl.getFlags();
      auto it = hdl.get();
      it_.store(it, std::memory_order_release);
      // Handles are fulfilled by threads different from the owners. Adjust
      // the refcount tracking accordingly. use the local copy to not make
      // this an atomic load check.
      if (it) {
        alloc_.adjustHandleCountForThread(-1);
      }
      {
        std::lock_guard<std::mutex> l(mtx_);
        if (onReadyCallback_) {
          // We will construct another handle that will be transferred to
          // another thread. So we will decrement a count locally to be back
          // to 0 on this thread. In the user thread, they must increment by
          // 1. It is done automatically if the user converted their ItemHandle
          // to a SemiFuture via toSemiFuture().
          auto itemHandle = hdl.clone();
          if (itemHandle) {
            alloc_.adjustHandleCountForThread(-1);
          }
          onReadyCallback_(std::move(itemHandle));
        }
      }
      baton_.post();
    }

    // @return      true iff we have the item
    bool isReady() const noexcept {
      return it_.load(std::memory_order_acquire) !=
             reinterpret_cast<const Item*>(kItemNotReady);
    }

    // Set the onReady callback
    // @param cb    ReadyCallback
    //
    // @return   callback function back if handle does not have waitContext_
    //                             or if waitContext_ is ready. Caller is
    //                             expected to call this function
    //           empty function    if waitContext exists and is not ready
    //
    ReadyCallback onReady(ReadyCallback&& callBack) {
      std::lock_guard<std::mutex> l(mtx_);
      if (isReady()) {
        return std::move(callBack);
      }
      onReadyCallback_ = std::move(callBack);
      // callback consumed, return empty function
      return ReadyCallback();
    }

    void releaseHandle() noexcept {
      // After @wait, callback is invoked. We don't have to worry about mutex.
      wait();
      if (it_.exchange(nullptr, std::memory_order_release) != nullptr) {
        alloc_.adjustHandleCountForThread(1);
      }
    }

    ~ItemWaitContext() override {
      if (!isReady()) {
        XDCHECK(false);
        XLOG(CRITICAL, "Destorying an unresolved handle");
        return;
      }
      auto it = it_.load(std::memory_order_acquire);
      if (it == nullptr) {
        return;
      }

      // if we have a wait context, we acquired the handle from another thread
      // that asynchronously created the handle. Fix up the thread local
      // refcount so that alloc_.release does not decrement it.
      alloc_.adjustHandleCountForThread(1);
      try {
        alloc_.release(it, /* isNascent */ false);
      } catch (const std::exception& e) {
        XLOGF(CRITICAL, "Failed to release {:#10x} : {}",
              static_cast<void*>(it), e.what());
      }
    }

   private:
    // we are waiting on Item* to be set to a value. One of the valid values is
    // nullptr. So choose something that we dont expect to indicate a ptr
    // state that is not valid.
    static constexpr uintptr_t kItemNotReady = 0x1221;
    mutable folly::fibers::Baton baton_; //< baton to wait on for the handle to
                                         // be "ready"
    std::mutex mtx_;                //< mutex to set and get onReadyCallback_
    ReadyCallback onReadyCallback_; //< callback invoked when "ready"
    std::atomic<Item*> it_{reinterpret_cast<Item*>(kItemNotReady)}; //< The item
    uint8_t flags_{}; //< flags associated with the handle generated by NvmCache
    CacheT& alloc_;   //< allocator instance
  };

  // clones Item handle. returns an empty handle if it is null.
  // @return HandleImpl   return a handle to this item
  // @throw std::overflow_error is the maximum item refcount is execeeded by
  //        creating this item handle.
  HandleImpl clone() const {
    HandleImpl hdl{};
    if (alloc_) {
      hdl = alloc_->acquire(get());
    }
    hdl.cloneFlags(*this);
    return hdl;
  }

  std::shared_ptr<ItemWaitContext> getItemWaitContext() const noexcept {
    return waitContext_;
  }

  FOLLY_ALWAYS_INLINE Item* getImpl() const noexcept {
    return waitContext_ ? waitContext_->get() : it_;
  }

  // Internal book keeping to track handles that correspond to items that are
  // not present in cache. This state is mutated, but does not affect the user
  // visible meaning of the item handle(public API). Hence this is const.
  // markNascent is set when we know the handle is constructed for an item that
  // is not inserted into the cache yet. we unmark the nascent flag when we know
  // the item was successfully inserted into the cache by the caller.
  void markNascent() const {
    flags_ |= static_cast<uint8_t>(HandleFlags::kNascent);
  }
  void unmarkNascent() const {
    flags_ &= ~static_cast<uint8_t>(HandleFlags::kNascent);
  }
  bool isNascent() const {
    return flags_ & static_cast<uint8_t>(HandleFlags::kNascent);
  }

  void markExpired() { flags_ |= static_cast<uint8_t>(HandleFlags::kExpired); }
  void markWentToNvm() {
    flags_ |= static_cast<uint8_t>(HandleFlags::kWentToNvm);
  }

  uint8_t getFlags() const {
    return waitContext_ ? waitContext_->getFlags() : flags_;
  }
  void cloneFlags(const HandleImpl& other) { flags_ = other.getFlags(); }

  Item* releaseItem() noexcept { return std::exchange(it_, nullptr); }

  // User of a handle can access cache via this accessor
  CacheT& getCache() const {
    XDCHECK(alloc_);
    return *alloc_;
  }

  // Handle which has the item already
  FOLLY_ALWAYS_INLINE HandleImpl(Item* it, CacheT& alloc) noexcept
      : alloc_(&alloc), it_(it) {}

  // handle that has a wait context allocated. Used for async handles
  // In this case, the it_ will be filled in asynchronously and mulitple
  // ItemHandles can wait on the one underlying handle
  explicit HandleImpl(CacheT& alloc) noexcept
      : alloc_(&alloc),
        it_(nullptr),
        waitContext_(std::make_shared<ItemWaitContext>(alloc)) {}

  // Only CacheAllocator and NvmCache can create non-default constructed handles
  friend CacheT;
  friend typename CacheT::NvmCacheT;

  // Object-cache's c++ allocator will need to create a zero refcount handle in
  // order to access CacheAllocator API. Search for this function for details.
  template <typename ItemHandle2, typename Item2, typename Cache2>
  friend ItemHandle2* objcacheInitializeZeroRefcountHandle(void* handleStorage,
                                                           Item2* it,
                                                           Cache2& alloc);

  // A handle is marked as nascent when it was not yet inserted into the cache.
  // However, user can override it by marking an item as "not nascent" even if
  // it's not inserted into the cache. Unmarking it means a not-yet-inserted
  // item will still be processed by RemoveCallback if user frees it. Today,
  // the only user who can do this is Cachelib's ObjectCache API to ensure the
  // correct RAII behavior for an object.
  template <typename ItemHandle2>
  friend void objcacheUnmarkNascent(const ItemHandle2& hdl);

  // Object-cache's c++ allocator needs to access CacheAllocator directly from
  // an item handle in order to access CacheAllocator APIs.
  template <typename ItemHandle2>
  friend typename ItemHandle2::CacheT& objcacheGetCache(const ItemHandle2& hdl);

  // instance of the cache this handle and item belong to.
  CacheT* alloc_ = nullptr;

  // pointer to item when the item does not have a wait context associated.
  Item* it_ = nullptr;

  // The waitContext allows an application to wait until the item is fetched.
  // This provides a future kind of interfaces (see ItemWaitContext for
  // details).
  std::shared_ptr<ItemWaitContext> waitContext_;

  mutable uint8_t flags_{};
};

template <typename T>
std::ostream& operator<<(std::ostream& os, const HandleImpl<T>& it) {
  if (it) {
    os << it->toString();
  } else {
    os << "nullptr";
  }
  return os;
}
} // namespace detail
} // namespace cachelib
} // namespace facebook

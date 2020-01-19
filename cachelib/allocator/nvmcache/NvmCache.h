#pragma once

#include <array>
#include <mutex>
#include <unordered_set>
#include <vector>

#include <folly/dynamic.h>
#include <folly/hash/Hash.h>
#include <folly/json.h>

#include "cachelib/allocator/nvmcache/DipperItem.h"
#include "cachelib/allocator/nvmcache/InFlightPuts.h"
#include "cachelib/allocator/nvmcache/ReqContexts.h"
#include "cachelib/allocator/nvmcache/TombStones.h"
#include "cachelib/allocator/nvmcache/WaitContext.h"
#include "cachelib/common/Exceptions.h"
#include "cachelib/common/Utils.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include "dipper/dipper.h"
#include "dipper/dipper_types.h"
#pragma GCC diagnostic pop

namespace facebook {
namespace cachelib {

// NvmCache is a key-value cache on flash. It is intended to be used
// along with a CacheAllocator to provide a uniform API to the application
// by abstracting away the details of ram and flash management.
template <typename C>
class NvmCache {
 public:
  using Item = typename C::Item;
  using ChainedItemIter = typename C::ChainedItemIter;

  // Context passed in encodeCb or decodeCb. If the item has children,
  // they are passed in the form of a folly::Range.
  struct EncodeDecodeContext {
    Item& item;
    folly::Range<ChainedItemIter> chainedItemRange;
  };

  // call back to encode an item to prepare for nvmcache if needed
  // @param it      item we are writing to nvmcache
  // @param allocs  pointer to chained allocs if the item has chained allocs
  // @return true if the item can be written. false if we need to fail
  using EncodeCB = std::function<bool(EncodeDecodeContext)>;

  // same as the above, but reverses the item back to its expected state if
  // needed.
  using DecodeCB = std::function<void(EncodeDecodeContext)>;

  // call back invoked everytime item inserted back from NVM into RAM
  // @param it      item we have inserted
  using MemoryInsertCB = std::function<void(const Item& it)>;

  // Callback to decrypt/encrypt an item. The expecation is that this
  // should never throw. Simply return nullptr in case of an error.
  // @param buf  buffer of data to be (de)encrypted
  // @return iobuf   buffer of (de)encrypted content
  //                 nullptr if (de)encryption failed
  using EncryptionCB =
      std::function<std::unique_ptr<folly::IOBuf>(folly::ByteRange buf)>;
  using DecryptionCB =
      std::function<std::unique_ptr<folly::IOBuf>(folly::ByteRange buf)>;

  using ItemHandle = typename C::ItemHandle;
  using DeleteTombStoneGuard = typename TombStones::Guard;
  using PutToken = typename InFlightPuts::PutToken;

  struct Config {
    folly::dynamic dipperOptions;

    // (Optional) enables the user to change some bits to prepare the item for
    // nvmcache serialization like fixing up pointers etc. Both must be encode
    // and decode must be specified or NOT specified. Encode and decode happen
    // before encryption and after decryption respectively if enabled.
    EncodeCB encodeCb{};
    DecodeCB decodeCb{};

    // (Optional) call back invoked item restored from NVM and inserted to
    // memory.
    MemoryInsertCB memoryInsertCb{};

    // (Optional) encrypt/decrypt payload before writing to NVM. Both must be
    // specified or NOT specified.
    EncryptionCB encryptCb{};
    DecryptionCB decryptCb{};

    // Whether or not store full alloc-class sizes into NVM device.
    // If true, only store the orignal size the user requested.
    bool truncateItemToOriginalAllocSizeInNvm{false};

    std::map<std::string, std::string> serialize() const {
      std::map<std::string, std::string> configMap;
      configMap["encodeCB"] = encodeCb ? "set" : "empty";
      configMap["decodeCb"] = decodeCb ? "set" : "empty";
      configMap["memoryInsertCb"] = memoryInsertCb ? "set" : "empty";
      configMap["encryptCb"] = encryptCb ? "set" : "empty";
      configMap["decryptCb"] = decryptCb ? "set" : "empty";
      configMap["truncateItemToOriginalAllocSizeInNvm"] =
          truncateItemToOriginalAllocSizeInNvm ? "true" : "false";
      for (auto& pair : dipperOptions.items()) {
        configMap["dipperOptions::" + pair.first.asString()] =
            (pair.second.isObject() || pair.second.isArray())
                ? folly::toJson(pair.second)
                : pair.second.asString();
      }
      return configMap;
    }

    Config validate() const {
      const bool hasEncodeCb = !!encodeCb;
      const bool hasDecodeCb = !!decodeCb;
      if (hasEncodeCb != hasDecodeCb) {
        throw std::invalid_argument(
            "Encode and Decode CBs must be both specified or both empty.");
      }

      const bool hasEncryptCb = !!encryptCb;
      const bool hasDecryptCb = !!decryptCb;
      if (hasEncryptCb != hasDecryptCb) {
        throw std::invalid_argument(
            "Encrypt and Decrypt CBs must be both specified or both empty.");
      }

      return *this;
    }
  };

  // @param c         the cache instance using nvmcache
  // @param config    the config for dipper store
  // @param truncate  if we should truncate the nvmcache store
  NvmCache(C& c, const Config& config, bool truncate);

  // Look up item by key
  // @param key         key to lookup
  // @return            ItemHandle
  ItemHandle find(folly::StringPiece key);

  // Try to mark the key as in process of being evicted from RAM to Dipper.
  // This is used to maintain the consistency between the RAM cache and
  // NvmCache when an item is transitioning from RAM to NvmCache in the
  // presence of concurrent gets and deletes from other threads.
  //
  // @param key         the key which is being evicted
  // @return            return true if successfully marked and the eviction
  //                    can proceed to queue a put in the future.  return
  //                    false otherwise
  PutToken createPutToken(folly::StringPiece key);

  // store the given item in dipper
  // @param hdl         handle to cache item to store in dipper. will be not
  //                    null
  // @param token       the put token for the item. this must have been
  //                    obtained before enqueueing the put to maintain
  //                    consistency
  void put(const ItemHandle& hdl, PutToken token);

  // returns the current state of whether nvmcache is enabled or not. nvmcache
  // can be disabled if the backend implementation ends up in a corrupt state
  bool isEnabled() const noexcept { return dipperEnabled_; }

  // creates a delete tombstone for the key. This will ensure that all
  // concurrent gets and puts to nvmcache can synchronize with an upcoming
  // delete to make the cache consistent.
  DeleteTombStoneGuard createDeleteTombStone(folly::StringPiece key);

  // remove an item by key
  // @param key         key to remove
  void remove(folly::StringPiece key);

  // peek the nvmcache without bringing the item into the cache. creates a
  // temporary item handle with the content of the nvmcache. this is intended
  // for debugging purposes
  //
  // @param key   the key for the cache item
  // @return    handle to the item in nvmcache if present. if not, nullptr is
  //            returned. if a handle is returned, it is not inserted into
  //            cache and is temporary.
  ItemHandle peek(folly::StringPiece key);

  // safely shut down the cache. must be called after stopping all concurrent
  // access to cache. using nvmcache after this will result in no-op.
  // Returns true if shutdown was performed properly, false otherwise.
  bool shutDown();

  // blocks until all in-flight ops are flushed to the device. To be used when
  // there are no more operations being enqueued.
  void flushPendingOps();

  // Flush and trigger compact keys for testing.
  void flushForTesting();

  // Obtain stats in a <string -> double> representation.
  std::unordered_map<std::string, double> getStatsMap() const {
    return store_->getStatsMap();
  }

  size_t getSize() const noexcept { return store_->dipperGetSize(); }

 private:
  detail::Stats& stats() { return cache_.stats_; }
  // creates the RAM item from DipperItem.
  //
  // @param key   key for the dipper item
  // @param dItem contents for the key
  //
  // @param   return an item handle allocated and initialized to the right state
  //          based on the dipperItem
  ItemHandle createItem(folly::StringPiece key, const DipperItem& dItem);

  // returns true if there is tombstone entry for the key.
  bool hasTombStone(folly::StringPiece key);

  std::unique_ptr<DipperItem> makeDipperItem(const ItemHandle& handle);

  // wrap an item into a blob for writing into dipper.
  Blob makeBlob(const Item& it);
  uint32_t getStorageSizeInNvm(const Item& it);

  // Holds all the necessary data to do an async dipper get
  // All of the supported operations aren't thread safe. The caller
  // needs to ensure thread safety
  struct GetCtx {
    NvmCache& cache;       //< the NvmCache instance
    const std::string key; //< key being fetched
    std::vector<std::shared_ptr<WaitContext<ItemHandle>>> waiters; // list of
                                                                   // waiters
    ItemHandle it; // will be set when Context is being filled
    bool cancelFill_;
    util::LatencyTracker tracker_;

    GetCtx(NvmCache& c,
           folly::StringPiece k,
           std::shared_ptr<WaitContext<ItemHandle>> ctx,
           util::LatencyTracker tracker)
        : cache(c),
          key(k.toString()),
          cancelFill_(false),
          tracker_(std::move(tracker)) {
      it.markWentToNvm();
      addWaiter(std::move(ctx));
    }

    ~GetCtx() {
      // prevent any further enqueue to waiters
      // Note: we don't need to hold locks since no one can enqueue
      // after this point.
      wakeUpWaiters();
    }

    // @return  key as StringPiece
    folly::StringPiece getKey() const { return {key.data(), key.length()}; }

    // record the item handle. Upon destruction we will wake up the waiters
    // and pass a clone of the handle to the callBack. By default we pass
    // a null handle
    void setItemHandle(ItemHandle _it) { it = std::move(_it); }

    void cancelFill() noexcept { cancelFill_ = true; }

    bool shouldCancelFill() const noexcept { return cancelFill_; }

    // enqueue a waiter into the waiter list
    // @param  waiter       WaitContext
    void addWaiter(std::shared_ptr<WaitContext<ItemHandle>> waiter) {
      XDCHECK(waiter);
      waiters.push_back(std::move(waiter));
    }

    // notify all pending waiters that are waiting for the fetch.
    void wakeUpWaiters() {
      bool refcountOverflowed = false;
      for (auto& w : waiters) {
        // If refcount overflowed earlier, then we will return miss to
        // all subsequent waitors.
        if (refcountOverflowed) {
          w->set(ItemHandle{});
          continue;
        }

        try {
          w->set(it.clone());
        } catch (const exception::RefcountOverflow&) {
          // We'll return a miss to the user's pending read,
          // so we should enqueue a delete via NvmCache.
          cache.cache_.remove(it);
          refcountOverflowed = true;
        }
      }
    }
  };

  // Erase entry for the ctx from the fill map
  // @param     ctx   ctx to erase
  void removeFromFillMap(const GetCtx& ctx) {
    auto key = ctx.getKey();
    auto lock = getFillLock(key);
    getFillMap(key).erase(key);
  }

  // Logs and disables dipper usage
  void disableDipper(const std::string& msg);

  // returns true if there is a concurrent get request in flight fetching from
  // nvm.
  bool mightHaveConcurrentFill(size_t shard, folly::StringPiece key);

  void cancelFillLocked(folly::StringPiece key, size_t shard);

  // map of concurrent fills by key. The key is a string piece wrapper around
  // GetCtx's std::string. This makes the lookups possible without
  // constructing a string key.
  using FillMap = std::
      unordered_map<folly::StringPiece, std::unique_ptr<GetCtx>, folly::Hash>;

  size_t getShardForKey(folly::StringPiece key) {
    return folly::Hash()(key) % kShards;
  }

  FillMap& getFillMapForShard(size_t shard) { return fills_[shard].fills_; }

  FillMap& getFillMap(folly::StringPiece key) {
    return getFillMapForShard(getShardForKey(key));
  }

  std::unique_lock<std::mutex> getFillLockForShard(size_t shard) {
    return std::unique_lock<std::mutex>(fillLock_[shard].fillLock_);
  }

  std::unique_lock<std::mutex> getFillLock(folly::StringPiece key) {
    return getFillLockForShard(getShardForKey(key));
  }

  void onGetComplete(GetCtx& ctx,
                     int error,
                     folly::ByteRange key,
                     folly::ByteRange value);

  void evictCB(folly::StringPiece key, folly::StringPiece val);

  // callback passed down to engine that needs to filter items out when doing
  // level compaction
  //
  // @param key      key for item
  // @param val      value for item
  //
  // @return  True if the item should be thrown away
  bool compactionFilterCb(folly::StringPiece key, folly::StringPiece value);

  const Config config_;
  C& cache_;                              //< cache allocator
  std::atomic<bool> dipperEnabled_{true}; //< switch to turn off/on dipper

  static constexpr size_t kShards = 8192;

  // threshold of classifying an object as large based on navy as the engine.
  static constexpr size_t kSmallObjectThreshold = 2048;

  // a map of all pending fills to prevent thundering herds
  struct {
    alignas(folly::hardware_destructive_interference_size) FillMap fills_;
  } fills_[kShards];

  // a map of fill locks for each shard
  struct {
    alignas(folly::hardware_destructive_interference_size) std::mutex fillLock_;
  } fillLock_[kShards];

  std::array<PutContexts, kShards> putContexts_;
  std::array<DelContexts, kShards> delContexts_;
  // co-ordination between in-flight evictions from cache that are not queued
  // to dipper and in-flight gets into nvmcache that are not yet queued.
  std::array<InFlightPuts, kShards> inflightPuts_;
  std::array<TombStones, kShards> tombstones_;

  std::unique_ptr<dipper::DipperStore> store_; //< dipper store
};

} // namespace cachelib
} // namespace facebook

#include "cachelib/allocator/nvmcache/NvmCache-inl.h"

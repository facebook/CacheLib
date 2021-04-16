// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once
#include <folly/io/IOBuf.h>
#include <folly/logging/xlog.h>

#include "cachelib/allocator/CacheVersion.h"
#include "cachelib/allocator/NvmCacheState.h"
#include "cachelib/allocator/nvmcache/NavySetup.h"
#include "cachelib/common/Exceptions.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/persistence/gen-cpp2/objects_types.h"
#include "cachelib/shm/ShmCommon.h"
#include "cachelib/shm/ShmManager.h"

namespace facebook::cachelib {

namespace tests {
class PersistenceManagerTest;
class PersistenceManagerMockTest;
} // namespace tests

namespace persistence {

constexpr uint32_t kDataBlockSize = 1 * 1024 * 1024; // 1MB

/**
 * Stream reader and writer APIs for PersistenceManager use.
 * read/write functions are called in a single thread.
 * Users should implement read/write functions with their
 * own storage backend, e.g. file, manifold, AWS.
 * Users should throw exception if any error happens during read/write,
 * exceptions are not handled by PersistenceManager so users should catch
 * and handle them properly.
 */
class PersistenceStreamReader {
 public:
  virtual ~PersistenceStreamReader() {}
  // data in IOBuf must remain valid until next read call,
  // it is recommanded to make IObuf maintain the lifetime of data.
  virtual folly::IOBuf read(size_t length) = 0;
  virtual char read() = 0;
};
class PersistenceStreamWriter {
 public:
  virtual ~PersistenceStreamWriter() {}
  // PersistenceManager guarantees the data in IOBuf
  // remain valid until flush() is called.
  virtual void write(folly::IOBuf buffer) = 0;
  virtual void write(char c) = 0;
  virtual void flush() = 0;
};

/**
 * PersistenceManager is to save cachelib instance to a remote storage, and
 * restore the cache cross host.
 *
 * User must shutdown cachelib instance before calling saveCache().
 * Any failure/exception during saveCache() will not affect local cache
 * instance, saveCache() can be retried with a clean writer. By calling
 * restoreCache() any existing cachelib data/metadata will be erased, and will
 * not be recovered on any failure.
 *
 * std::runtime_error will be thrown upon any error happens during save/restore.
 *
 * To recover cache, user should attach the cachelib instance (with
 * SharedMemAttach constructor) after restore. The attach might fail if the
 * supplied cachelib config is incompatible with the config of the restored
 * cache content. See the following for a list of configs that must NOT change
 * across restarts.
 *
 *  - cacheSize
 *  - cacheName
 *  - cacheDir
 *  - usePosixForShm
 *  - isCompactCache
 *  - isNvmCacheEncryption
 *  - isNvmCacheTruncateAllocSize
 *  - accessConfig.numBuckets
 *  - accessConfig.pageSize
 *  - chainedItemAccessConfig.numBuckets
 *  - nvmConfig.dipperOptions["dipper_navy_file_name"]
 *  - nvmConfig.dipperOptions["dipper_navy_raid_paths"]
 *  - nvmConfig.dipperOptions["dipper_navy_file_size"]
 */
class PersistenceManager {
 public:
  template <typename CachelibConfig>
  explicit PersistenceManager(const CachelibConfig& config) {
    versions_.allocatorVersion_ref() = kCachelibVersion;
    versions_.ramFormatVerson_ref() = kCacheRamFormatVersion;
    versions_.nvmFormatVersion_ref() = kCacheNvmFormatVersion;
    versions_.persistenceVersion_ref() = kPersistenceVersion;

    config_.cacheName_ref() = config.getCacheName();
    cacheDir_ = config.getCacheDir();

    CACHELIB_CHECK_THROW(config.isUsingPosixShm(),
                         "Only POSIX is supported to persist");
    CACHELIB_CHECK_THROW(config.accessConfig.getPageSize() == PageSizeT::NORMAL,
                         "Only default PageSize is supported to persist");

    if (config.nvmConfig.has_value()) {
      const auto& dipper = config.nvmConfig->dipperOptions;
      if (!dipper.empty()) {
        validatePathConfig(dipper);

        if (usesSimpleFile(dipper)) {
          navyFiles_.push_back(getNavyFilePath(dipper));
        } else if (usesRaidFiles(dipper)) {
          navyFiles_ = getNavyRaidPaths(dipper);
        }
        navyFileSize_ = getNavyFileSize(dipper);
      } else if (config.nvmConfig->navyConfig.isEnabled()) {
        const auto& navyConfig = config.nvmConfig->navyConfig;
        if (navyConfig.usesSimpleFile()) {
          navyFiles_.push_back(navyConfig.getFileName());
        } else if (navyConfig.usesRaidFiles()) {
          navyFiles_ = navyConfig.getRaidPaths();
        }
        navyFileSize_ = navyConfig.getFileSize();
      }
    }
  }

  /* save cache metadata/data, call writer.write() */
  void saveCache(PersistenceStreamWriter& writer);
  /* call reader.read(), restore cache metadata/data to memory/disk */
  void restoreCache(PersistenceStreamReader& reader);

  const static char DATA_BEGIN_CHAR;
  const static char DATA_MARK_CHAR;
  const static char DATA_END_CHAR;

 private:
  folly::IOBuf makeHeader(PersistenceType, size_t);

  void saveFile(PersistenceStreamWriter&,
                PersistenceType,
                const folly::StringPiece);

  void restoreFile(const folly::IOBuf&, const folly::StringPiece);

  // returns the unique_ptr to hold lifetime of the memory
  // so that writer.flush is not required for each shm
  std::unique_ptr<ShmSegment> saveShm(PersistenceStreamWriter&,
                                      PersistenceType,
                                      const std::string&);

  void saveDataInBlocks(PersistenceStreamWriter&, const ShmAddr&);
  void restoreDataFromBlocks(PersistenceStreamReader&, uint8_t*, size_t);

  void deserializeAndValidateVersions(const folly::IOBuf&);

  template <typename T>
  static const T& cast(const uint8_t* p) {
    return *reinterpret_cast<const T*>(p);
  }

  template <typename T>
  static T deserialize(const folly::IOBuf& buf) {
    Deserializer deserializer(buf.data(), buf.tail());
    return deserializer.deserialize<T>();
  }

  constexpr static int32_t kPersistenceVersion = 0;

  CacheLibVersions versions_;
  PersistCacheLibConfig config_;

  // cache dir is not persist x-host
  // but we need this info during persist/restore.
  std::string cacheDir_;
  // navy file path and size to save/restore navy data
  std::vector<std::string> navyFiles_;
  uint64_t navyFileSize_;

  friend class facebook::cachelib::tests::PersistenceManagerTest;
  friend class facebook::cachelib::tests::PersistenceManagerMockTest;
};

} // namespace persistence
} // namespace facebook::cachelib

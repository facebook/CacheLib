#pragma once

#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Range.h>
#pragma GCC diagnostic pop
#include <folly/File.h>

namespace facebook {
namespace cachelib {

class NvmCacheState {
 public:
  // file name to indicate that the nvmcache should be dropped the next time
  // we instantiate it.
  // @param  cacheDir   the directory we use for the cache state
  //
  // @return a std::string with the path for the control file
  static std::string getFileForNvmCacheDrop(folly::StringPiece cacheDir);

  // Intialize the object for the cacheDir.
  explicit NvmCacheState(const std::string& cacheDir,
                         bool encryptionEnabled,
                         bool truncateAllocSize);

  // returns the time when the nvmcache associated with the cacheDir was
  // created
  time_t getCreationTime() const;

  // returns true if NvmCache should start without any previous state.
  // This is true if `shouldDropNvmCache()` or `wasCleanShutDown()` is true
  bool shouldStartFresh() const;

  // return true if the cache dir indicates that we should drop nvm cache
  // This is used for users of cachelib to signal to cachelib to drop the
  // nvmcache externally on the next run.
  bool shouldDropNvmCache() const;

  // return true if we previously recorded that nvmcache was safely shutdown
  bool wasCleanShutDown() const;

  // mark the nvmcache as safely shutdown.
  void markSafeShutDown();

  // clear the previous state associated with the nvmcache
  void clearPrevState();

  // mark the state as truncated and similar to a freshly created nvmcache.
  void markTruncated();

  // print the current state for debugging purposes
  std::string toString() const;

 private:
  // constructs the file name for the corresponding name
  std::string getFileNameFor(folly::StringPiece name) const;

  void restoreState();
  void restoreStateNew();
  void restoreStateLegacy();

  void markSafeShutDownNew();
  void markSafeShutDownLegacy();

  // the directory identifying the state
  const std::string cacheDir_;

  // should nvm cache be dropped
  bool shouldDropNvmCache_{false};

  // was nvm cache cleanly shut down previously
  bool wasCleanshutDown_{false};

  // time when NvmCache was first created
  time_t creationTime_{0};

  // is encryption enabled
  bool encryptionEnabled_{false};

  // if enabled, only store user-requested size into nvm cache
  bool truncateAllocSize_{false};

  // a stream for writing metadata is created and kept open throughout the
  // lifetime of this object. This resolves any issue with a process's
  // permission to write metadata if the user downgrades the process's perm.
  std::unique_ptr<folly::File> metadataFile_;
};

} // namespace cachelib
} // namespace facebook

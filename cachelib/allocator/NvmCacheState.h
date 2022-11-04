/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Range.h>
#pragma GCC diagnostic pop
#include <folly/File.h>

namespace facebook {
namespace cachelib {

// State class for Nvm cache
class NvmCacheState {
 public:
  // file name to indicate that the nvmcache should be dropped the next time
  // we instantiate it.
  // @param  cacheDir   the directory we use for the cache state
  //
  // @return a std::string with the path for the control file
  static std::string getFileForNvmCacheDrop(folly::StringPiece cacheDir);

  // cacheDir/NvmCacheState, file that store NVM cache meta data.
  // @param  cacheDir   the directory we use for the cache state
  //
  // @return a std::string with the path for the NvmCacheState file
  static std::string getNvmCacheStateFilePath(folly::StringPiece cacheDir);

  // Intialize the object for the cacheDir.
  explicit NvmCacheState(uint32_t currentTimeSecs,
                         const std::string& cacheDir,
                         bool encryptionEnabled,
                         bool truncateAllocSize);

  // returns the time when the nvmcache associated with the cacheDir was
  // created
  time_t getCreationTime() const;

  // returns true if NvmCache should start without any previous state.
  // This is true if `shouldDropNvmCache()` is true or `wasCleanShutDown()` is
  // false
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

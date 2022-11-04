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

#include <folly/io/RecordIO.h>
#include <folly/logging/xlog.h>

#include <fstream>
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wconversion"
#include <folly/Format.h>
#pragma GCC diagnostic pop

#include "cachelib/allocator/CacheVersion.h"
#include "cachelib/allocator/NvmCacheState.h"
#include "cachelib/allocator/serialize/gen-cpp2/objects_types.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/common/Time.h"
#include "cachelib/common/Utils.h"

namespace facebook {
namespace cachelib {

namespace {
const folly::StringPiece kNvmCacheState = "NvmCacheState";
const folly::StringPiece kShouldDropNvmCache = "IceRoll";

bool fileExists(const std::string& file) {
  return util::getStatIfExists(file, nullptr);
}

std::string constructFilePath(folly::StringPiece cacheDir,
                              folly::StringPiece name) {
  return folly::sformat("{}/{}", cacheDir, name);
}

serialization::NvmCacheMetadata loadMetadata(folly::StringPiece fileName) {
  folly::File shutDownFile{fileName};
  folly::RecordIOReader rr{std::move(shutDownFile)};
  auto metadataIoBuf = folly::IOBuf::copyBuffer(rr.begin()->first);
  if (metadataIoBuf->length() == 0) {
    throw std::runtime_error(
        folly::sformat("no content in file: {}", fileName));
  }
  Deserializer deserializer{metadataIoBuf->data(),
                            metadataIoBuf->data() + metadataIoBuf->length()};
  return deserializer.deserialize<serialization::NvmCacheMetadata>();
}

void saveMetadata(const folly::File& file,
                  const serialization::NvmCacheMetadata& metadata) {
  auto metadataIoBuf = Serializer::serializeToIOBuf(metadata);
  folly::File shutDownFile{file.fd()};
  folly::RecordIOWriter rw{std::move(shutDownFile)};
  rw.write(std::move(metadataIoBuf));
}
} // namespace

std::string NvmCacheState::getNvmCacheStateFilePath(
    folly::StringPiece cacheDir) {
  return constructFilePath(cacheDir, kNvmCacheState);
}

NvmCacheState::NvmCacheState(uint32_t currentTimeSecs,
                             const std::string& cacheDir,
                             bool encryptionEnabled,
                             bool truncateAllocSize)
    : cacheDir_(cacheDir),
      creationTime_{currentTimeSecs},
      encryptionEnabled_{encryptionEnabled},
      truncateAllocSize_{truncateAllocSize} {
  if (cacheDir_.empty()) {
    return;
  }

  if (!fileExists(cacheDir_)) {
    util::makeDir(cacheDir_);
  } else if (!util::isDir(cacheDir_)) {
    throw std::invalid_argument(
        folly::sformat("Expected {} to be directory", cacheDir_));
  }

  restoreState();

  // Keep a stream open for the metadata
  metadataFile_ = std::make_unique<folly::File>(getFileNameFor(kNvmCacheState),
                                                O_CREAT | O_TRUNC | O_RDWR);
  XDCHECK(metadataFile_);
}

void NvmCacheState::restoreState() {
  // Read previous instance state from nvm state file
  try {
    shouldDropNvmCache_ = fileExists(getFileNameFor(kShouldDropNvmCache));

    if (!fileExists(getFileNameFor(kNvmCacheState))) {
      // No nvm cache state supplied, we return early. Nvm cache will still be
      // started afresh due to wasCleanshutDown_ == false
      return;
    }

    auto metadata = loadMetadata(getFileNameFor(kNvmCacheState));
    wasCleanshutDown_ = *metadata.safeShutDown();

    if (!shouldStartFresh()) {
      if (*metadata.nvmFormatVersion() == kCacheNvmFormatVersion &&
          encryptionEnabled_ == *metadata.encryptionEnabled() &&
          truncateAllocSize_ == *metadata.truncateAllocSize()) {
        creationTime_ = *metadata.creationTime();
      } else {
        XLOGF(ERR,
              "Expected nvm format version {}, but found {}. Expected "
              "encryption to be {}, but found {}. Expected truncateAllocSize "
              "to be {}, but found {}. Dropping NvmCache",
              kCacheNvmFormatVersion, *metadata.nvmFormatVersion(),
              encryptionEnabled_ ? "true" : "false",
              *metadata.encryptionEnabled() ? "true" : "false",
              truncateAllocSize_ ? "true" : "false",
              *metadata.truncateAllocSize() ? "true" : "false");
        shouldDropNvmCache_ = true;
      }
    }
  } catch (const std::exception& ex) {
    XLOGF(ERR, "unable to deserialize nvm metadata file: {}", ex.what());
    shouldDropNvmCache_ = true;
  }
}

bool NvmCacheState::shouldStartFresh() const {
  return shouldDropNvmCache() || !wasCleanShutDown();
}

bool NvmCacheState::shouldDropNvmCache() const { return shouldDropNvmCache_; }

bool NvmCacheState::wasCleanShutDown() const { return wasCleanshutDown_; }

time_t NvmCacheState::getCreationTime() const { return creationTime_; }

void NvmCacheState::clearPrevState() {
  auto dropFile = getFileNameFor(kShouldDropNvmCache);
  if (::unlink(dropFile.data()) != 0 && errno != ENOENT) {
    util::throwSystemError(errno, "Failed to delete nvm run file");
  }
  XDCHECK(metadataFile_);
  ftruncate(metadataFile_->fd(), 0);
}

void NvmCacheState::markSafeShutDown() {
  XDCHECK(metadataFile_);
  serialization::NvmCacheMetadata metadata;
  *metadata.nvmFormatVersion() = kCacheNvmFormatVersion;
  *metadata.creationTime() = creationTime_;
  *metadata.safeShutDown() = true;
  *metadata.encryptionEnabled() = encryptionEnabled_;
  *metadata.truncateAllocSize() = truncateAllocSize_;
  saveMetadata(*metadataFile_, metadata);
}

std::string NvmCacheState::getFileNameFor(folly::StringPiece name) const {
  return constructFilePath(cacheDir_, name);
}

std::string NvmCacheState::getFileForNvmCacheDrop(folly::StringPiece cacheDir) {
  return constructFilePath(cacheDir, kShouldDropNvmCache);
}

std::string NvmCacheState::toString() const {
  return folly::sformat("cleanShutDown={}, shouldDrop={}, creationTime={}",
                        wasCleanShutDown(),
                        shouldDropNvmCache(),
                        getCreationTime());
}

void NvmCacheState::markTruncated() {
  wasCleanshutDown_ = false;
  creationTime_ = util::getCurrentTimeSec();
}

} // namespace cachelib

} // namespace facebook

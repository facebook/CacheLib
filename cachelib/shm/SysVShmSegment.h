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
#include <sys/ipc.h>

#include <string>

#include "cachelib/shm/ShmCommon.h"

namespace facebook {
namespace cachelib {

constexpr int kInvalidShmId = -1;
constexpr key_t kInvalidKey = (key_t)-1;

/* Provides an interface to use the sys-V shared memory API. Segments are
 * identified through by a key_t generated using ftok. Segments once created
 * can be mapped. Once mapped into the address space, they need to be detached
 * on exit. Segments live on until they are marked for removal and all address
 * mappings are detached. Once a segment is marked for removal, new segment
 * with the same key can be created and all further attaches will map to the
 * new segment.
 **/
class SysVShmSegment : public ShmBase {
 public:
  using KeyType = key_t;
  // attaches to an existing sysv segment with the given key
  //
  // @param key   the key for the segment.
  // @param opts  the options for attaching the segment.
  SysVShmSegment(ShmAttachT, const std::string& name, ShmSegmentOpts opts = {});

  // creates a news segment with the given key.
  //
  // @param key   the key for the segment.
  // @param size  the size of the segment. This will be rounded up to the
  //              nearest page size
  // @param opts  the options for creating the segment.
  SysVShmSegment(ShmNewT,
                 const std::string& name,
                 size_t size,
                 ShmSegmentOpts opts = {});

  ~SysVShmSegment() override try {
    // delete the reference mapping so the segment can be deleted if its
    // marked to be.
    deleteReferenceMapping();
  } catch (const std::system_error&) {
  }

  std::string getKeyStr() const noexcept override {
    return std::to_string(key_);
  }

  // Size of the segment
  size_t getSize() const override;

  // Marks the segment to be removed once the last address mapping is
  // detached.
  void markForRemoval() override;

  // Attaches the given address to the shared memory segment. The size
  // of the mapping is the size of the segment. Address needs to be page
  // aligned.
  // @param addr  the start of the address for attaching.
  // @return    the address where  the segment was mapped to. This will be same
  //            as addr if addr is not nullptr
  // @throw std::system_error with EINVAL if the segment is not valid or
  //        address/length are not page aligned.
  void* mapAddress(void* addr) const override;
  void unMap(void* addr) const override;

  // useful for removing without attaching
  // @return true if the segment existed. false otherwise
  static bool removeByName(const std::string& name);

 private:
  // returns the key identifier for the given name.
  static KeyType createKeyForName(const std::string& name) noexcept;

  static int createNewSegment(key_t key,
                              size_t size,
                              const ShmSegmentOpts& opts);
  static int attachToExisting(key_t key, const ShmSegmentOpts& opts);
  void lockPagesInMemory() const;
  void createReferenceMapping();
  void deleteReferenceMapping() const;
  void memBind(void* addr) const;

  //  the key identifier for the shared memory
  KeyType key_{kInvalidKey};

  // the shmid corresponding to this shared memory segment.
  int shmid_{kInvalidShmId};
};
} // namespace cachelib
} // namespace facebook

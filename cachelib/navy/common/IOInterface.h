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

#include <liburing.h>
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {

class NvmeData;

enum InterfaceDDir {
  DDIR_READ = 0,
  DDIR_WRITE = 1,
};

class IOData {
 public:
  IOData() = default;
  IOData& operator=(const IOData&) = default;

  explicit IOData(int fd, uint64_t offset, uint32_t size, uint8_t *buf)
    : fd_{fd}, offset_{offset}, size_{size}, buf_{buf} {}

  int fd() const { return fd_;}
  uint64_t offset() const { return offset_;}
  uint32_t size() const { return size_;}
  uint8_t* buf() const { return buf_;}
  uint8_t dtype() const { return dType_;}
  uint16_t dspec() const { return dSpec_;}

  void setDirectiveData(uint8_t dType, uint16_t dSpec) {
    dType_ = dType;
    dSpec_ = dSpec;
  }

 private:
  int fd_;
  uint64_t offset_{0};
  uint32_t size_{0};
  uint8_t *buf_{};
  uint8_t dType_{0};
  uint16_t dSpec_{0};
};

// IOInterface is an abstraction layer for IO interface access
class IOInterface {
 public:
  explicit IOInterface() {}
  bool nvmeIO(const IOData ioData, const NvmeData nvmeData, InterfaceDDir dir);

 protected:
  virtual bool nvmeIOImpl(const IOData ioData, const NvmeData nvmeData,
                              InterfaceDDir dir) = 0;
};

std::unique_ptr<IOInterface> createIOUringNvmeInterface();
} // namespace navy
} // namespace cachelib
} // namespace facebook

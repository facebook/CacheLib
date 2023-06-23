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

#include <folly/File.h>
#include <folly/Format.h>

#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <errno.h>

#include <cstring>
#include <numeric>
#include "cachelib/navy/common/IOInterface.h"
#include "cachelib/navy/common/NvmeInterface.h"

namespace facebook {
namespace cachelib {
namespace navy {

void prepNvmeUringCmd(struct nvme_uring_cmd *cmd, const IOData ioData,
    const NvmeData nvmeData, InterfaceDDir dir) {
  memset(cmd, 0, sizeof(struct nvme_uring_cmd));
  if (dir == DDIR_READ)
    cmd->opcode = nvme_cmd_read;
  else
    cmd->opcode = nvme_cmd_write;

  uint64_t sLba = ioData.offset() >> nvmeData.lbaShift();
  uint32_t nLb  = (ioData.size() >> nvmeData.lbaShift()) - 1;

  /* cdw10 and cdw11 represent starting lba */
  cmd->cdw10 = sLba & 0xffffffff;
  cmd->cdw11 = sLba >> 32;
  /* cdw12 represent number of lba's for read/write */
  cmd->cdw12 = (ioData.dtype() & 0xFF) << 20 | nLb;
  cmd->cdw13 = (ioData.dspec() << 16);
  cmd->addr = (uint64_t)ioData.buf();
  cmd->data_len = ioData.size();

  cmd->nsid = nvmeData.nsId();
}

bool submitNvmeUringCmd(struct io_uring *ring, const IOData ioData,
    const NvmeData nvmeData, InterfaceDDir dir) {
  struct io_uring_sqe *sqe;
  struct io_uring_cqe *cqe;
  struct nvme_uring_cmd *cmd;
  const __u64 token = 0xDEADBEEF;
  int32_t status = -1;

  sqe = io_uring_get_sqe(ring);
  if(sqe == nullptr) {
    throw std::invalid_argument("Uring SQE is NULL!");
  }
  sqe->fd = ioData.fd();
  sqe->user_data = token;
  sqe->opcode = IORING_OP_URING_CMD ;
  sqe->cmd_op = NVME_URING_CMD_IO ;
  cmd = (struct nvme_uring_cmd *)&sqe->cmd;
  if(cmd == nullptr) {
    throw std::invalid_argument("Uring cmd is NULL!");
  }

  prepNvmeUringCmd(cmd, ioData, nvmeData, dir);

  io_uring_submit(ring);
  io_uring_wait_cqe(ring, &cqe);
  if(cqe == nullptr) {
    throw std::invalid_argument("Uring CQE is NULL!");
  }

  switch (cqe->user_data) {
    case token:
    {
      status = cqe->res;
      int64_t result1 = cqe->big_cqe[0];
      int64_t result2 = cqe->big_cqe[1];
      if(status != 0) {
        XLOGF(INFO, "Completion Err for IO:status: {} res1: {} res2: {}\n",
          status, result1, result2);
      }
      io_uring_cqe_seen(ring, cqe);
    }
    break;
    default:
      XLOGF(INFO, "invalid user data {}\n", cqe->user_data);
  }

  return (status == 0);
}

// This makes sure that every thread initializes it's own copy of uring
thread_local bool needInit_{true};
thread_local struct io_uring uring_;

class IOUringPassthruInterface final : public IOInterface {
public:
  IOUringPassthruInterface() {}
  IOUringPassthruInterface(const IOUringPassthruInterface&) = delete;
  IOUringPassthruInterface& operator=(const IOUringPassthruInterface&) = delete;

  ~IOUringPassthruInterface() {}

private:
  bool nvmeIOImpl(const IOData ioData, const NvmeData nvmeData,
                          InterfaceDDir dir) {
    if(needInit_) {
      nvmeUringInit();
      needInit_ = false;
    }
    return submitNvmeUringCmd(&uring_, ioData, nvmeData, dir);
  }

  bool nvmeUringInit() {
    struct io_uring_params p = {};
    const unsigned int qdepth = 128;

    p.flags = IORING_SETUP_SQE128 | IORING_SETUP_CQE32;
    int ret = io_uring_queue_init(qdepth, &uring_, p.flags);
    if(ret != 0) {
      throw std::invalid_argument("URING init error!");
    }

    return (ret == 0);
  }

  void nvmeUringExit() {
    io_uring_queue_exit(&uring_);
  }
};

bool IOInterface::nvmeIO(const IOData ioData, const NvmeData nvmeData, InterfaceDDir dir) {
  return nvmeIOImpl(ioData, nvmeData, dir);
}

std::unique_ptr<IOInterface> createIOUringNvmeInterface() {
  return std::make_unique<IOUringPassthruInterface>();
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

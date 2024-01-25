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

#include <folly/File.h>
#include <folly/experimental/io/AsyncBase.h>
#include <folly/experimental/io/IoUring.h>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"

namespace facebook {
namespace cachelib {
namespace navy {
#ifndef CACHELIB_IOURING_DISABLE

// Reference: https://github.com/axboe/fio/blob/master/engines/nvme.h
// If the uapi headers installed on the system lacks nvme uring command
// support, use the local version to prevent compilation issues.
#ifndef CONFIG_NVME_URING_CMD
struct nvme_uring_cmd {
  __u8 opcode;
  __u8 flags;
  __u16 rsvd1;
  __u32 nsid;
  __u32 cdw2;
  __u32 cdw3;
  __u64 metadata;
  __u64 addr;
  __u32 metadata_len;
  __u32 data_len;
  __u32 cdw10;
  __u32 cdw11;
  __u32 cdw12;
  __u32 cdw13;
  __u32 cdw14;
  __u32 cdw15;
  __u32 timeout_ms;
  __u32 rsvd2;
};

#define NVME_URING_CMD_IO _IOWR('N', 0x80, struct nvme_uring_cmd)
#define NVME_URING_CMD_IO_VEC _IOWR('N', 0x81, struct nvme_uring_cmd)
#endif /* CONFIG_NVME_URING_CMD */

#define NVME_DEFAULT_IOCTL_TIMEOUT 0

enum nvme_io_mgmt_recv_mo {
  NVME_IO_MGMT_RECV_RUH_STATUS = 0x1,
};

struct nvme_fdp_ruh_status_desc {
  uint16_t pid;
  uint16_t ruhid;
  uint32_t earutr;
  uint64_t ruamw;
  uint8_t rsvd16[16];
};

struct nvme_fdp_ruh_status {
  uint8_t rsvd0[14];
  uint16_t nruhsd;
  struct nvme_fdp_ruh_status_desc ruhss[];
};

enum nvme_io_opcode {
  nvme_cmd_write = 0x01,
  nvme_cmd_read = 0x02,
  nvme_cmd_io_mgmt_recv = 0x12,
  nvme_cmd_io_mgmt_send = 0x1d,
};

// NVMe specific data for a device
//
// This is needed because FDP-IO have to be sent through Io_Uring_Cmd interface.
// So NVMe data is needed for initialization and IO cmd formation.
class NvmeData {
 public:
  NvmeData() = default;
  NvmeData& operator=(const NvmeData&) = default;

  explicit NvmeData(int nsId,
                    uint32_t lbaShift,
                    uint32_t maxTfrSize,
                    uint64_t startLba)
      : nsId_(nsId),
        lbaShift_(lbaShift),
        maxTfrSize_(maxTfrSize),
        startLba_(startLba) {}

  // NVMe Namespace ID
  int nsId() const { return nsId_; }

  // LBA shift number to calculate blocksize
  uint32_t lbaShift() const { return lbaShift_; }

  // Get the max transfer size of NVMe device.
  uint32_t getMaxTfrSize() { return maxTfrSize_; }

  // Start LBA of the disk partition.
  // It will be 0, if there is no partition and just an NS.
  uint64_t partStartLba() const { return startLba_; }

 private:
  int nsId_;
  uint32_t lbaShift_;
  uint32_t maxTfrSize_;
  uint64_t startLba_;
};
#endif

// FDP specific info and handling
//
// This embeds the FDP semantics and specific io-handling.
// Note: IO with FDP semantics need to be sent through Io_Uring_cmd interface
// as of now; and not supported through conventional block interfaces.
class FdpNvme {
 public:
  explicit FdpNvme(const std::string& fileName);

  FdpNvme(const FdpNvme&) = delete;
  FdpNvme& operator=(const FdpNvme&) = delete;

#ifndef CACHELIB_IOURING_DISABLE
  // Allocates an FDP specific placement handle. This handle will be
  // interpreted by the device for data placement.
  int allocateFdpHandle();

  // Get the max IO transfer size of NVMe device.
  uint32_t getMaxIOSize() { return nvmeData_.getMaxTfrSize(); }

  // Get the NVMe specific info on this device.
  NvmeData& getNvmeData() { return nvmeData_; }

  // Reads FDP status descriptor into Buffer
  Buffer nvmeFdpStatus();

  // Prepares the Uring_Cmd sqe for read command.
  void prepReadUringCmdSqe(struct io_uring_sqe& sqe,
                           void* buf,
                           size_t size,
                           off_t start);

  // Prepares the Uring_Cmd sqe for write command with FDP handle.
  void prepWriteUringCmdSqe(struct io_uring_sqe& sqe,
                            void* buf,
                            size_t size,
                            off_t start,
                            int handle);

 private:
  // Open Nvme Character device for the given block dev @fileName.
  folly::File openNvmeCharFile(const std::string& fileName);

  // Prepares the Uring_Cmd sqe for read/write command with FDP directives.
  void prepFdpUringCmdSqe(struct io_uring_sqe& sqe,
                          void* buf,
                          size_t size,
                          off_t start,
                          uint8_t opcode,
                          uint8_t dtype,
                          uint16_t dspec);

  // Get FDP PlacementID for a NVMe NS specific PHNDL
  uint16_t getFdpPID(uint16_t fdpPHNDL) { return placementIDs_[fdpPHNDL]; }

  // Reads NvmeData for a NVMe device
  NvmeData readNvmeInfo(const std::string& blockDevice);

  // Initialize the FDP device and populate necessary info.
  void initializeFDP(const std::string& blockDevice);

  // Generic NVMe IO mgmnt receive cmd
  int nvmeIOMgmtRecv(uint32_t nsid,
                     void* data,
                     uint32_t data_len,
                     uint8_t op,
                     uint16_t op_specific);

  // 0u is considered as the default placement ID
  static constexpr uint16_t kDefaultPIDIdx = 0u;

  // The mapping table of PHNDL: PID in a Namespace
  std::vector<uint16_t> placementIDs_{};

  uint16_t maxPIDIdx_{0};
  uint16_t nextPIDIdx_{kDefaultPIDIdx + 1};
  NvmeData nvmeData_{};
  // File handle for IO with FDP directives. Since FDP IO requires the use of
  // NVMe character device interface, a separate file instance is kept from
  // that of FileDevice.
  folly::File file_;
#endif
};

} // namespace navy
} // namespace cachelib
} // namespace facebook

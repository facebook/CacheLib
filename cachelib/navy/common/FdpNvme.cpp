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

#include "cachelib/navy/common/FdpNvme.h"

#include <errno.h>
#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>

#include <cstring>
#include <regex>

#ifndef CACHELIB_IOURING_DISABLE

namespace facebook {
namespace cachelib {
namespace navy {

FdpNvme::FdpNvme(const std::string& bdevName)
    : file_(openNvmeCharFile(bdevName)) {
  initializeFDP(bdevName);
  XLOGF(INFO, "Initialized NVMe FDP Device on file: {}", bdevName);
}

int FdpNvme::allocateFdpHandle() {
  uint16_t phndl;

  // Get NS specific Fdp Placement Handle(PHNDL)
  if (nextPIDIdx_ <= maxPIDIdx_) {
    phndl = nextPIDIdx_++;
  } else {
    phndl = kDefaultPIDIdx;
  }

  XLOGF(INFO, "Allocated an FDP handle {}", phndl);
  return static_cast<int>(phndl);
}

void FdpNvme::initializeFDP(const std::string& bdevName) {
  nvmeData_ = readNvmeInfo(bdevName);

  Buffer buffer = nvmeFdpStatus();
  struct nvme_fdp_ruh_status* ruh_status =
      reinterpret_cast<struct nvme_fdp_ruh_status*>(buffer.data());

  if (!ruh_status->nruhsd) {
    throw std::invalid_argument("Failed to initialize FDP; nruhsd is 0");
  }
  placementIDs_.reserve(ruh_status->nruhsd);
  maxPIDIdx_ = ruh_status->nruhsd - 1;
  for (uint16_t i = 0; i <= maxPIDIdx_; ++i) {
    placementIDs_[i] = ruh_status->ruhss[i].pid;
  }

  XLOGF(DBG, "Creating NvmeFdp, fd: {}, Num_PID: {}, 1st PID: {}, Last PID: {}",
        file_.fd(), maxPIDIdx_ + 1, placementIDs_[0],
        placementIDs_[maxPIDIdx_]);
}

// NVMe IO Mnagement Receive fn for specific config reading
int FdpNvme::nvmeIOMgmtRecv(uint32_t nsid,
                            void* data,
                            uint32_t data_len,
                            uint8_t op,
                            uint16_t op_specific) {
  // IO management command details
  uint32_t cdw10 = (op & 0xf) | (op_specific & 0xff << 16);
  uint32_t cdw11 = (data_len >> 2) - 1; // cdw11 is 0 based

  struct nvme_passthru_cmd cmd = {
      .opcode = nvme_cmd_io_mgmt_recv,
      .nsid = nsid,
      .addr = (uint64_t)(uintptr_t)data,
      .data_len = data_len,
      .cdw10 = cdw10,
      .cdw11 = cdw11,
      .timeout_ms = NVME_DEFAULT_IOCTL_TIMEOUT,
  };

  return ioctl(file_.fd(), NVME_IOCTL_IO_CMD, &cmd);
}

// struct nvme_fdp_ruh_status is a variable sized object; so using Buffer.
Buffer FdpNvme::nvmeFdpStatus() {
  struct nvme_fdp_ruh_status hdr;
  int err;

  // Read FDP ruh status header to get Num_RUH Status Descriptors
  err = nvmeIOMgmtRecv(nvmeData_.nsId(), &hdr, sizeof(hdr),
                       NVME_IO_MGMT_RECV_RUH_STATUS, 0);
  if (err) {
    throw std::system_error(
        errno,
        std::system_category(),
        folly::sformat("failed to get ruh status header, fd: {}", file_.fd()));
  }

  auto size = sizeof(struct nvme_fdp_ruh_status) +
              (hdr.nruhsd * sizeof(struct nvme_fdp_ruh_status_desc));
  auto buffer = Buffer(size);

  // Read FDP RUH Status
  err = nvmeIOMgmtRecv(nvmeData_.nsId(), buffer.data(), size,
                       NVME_IO_MGMT_RECV_RUH_STATUS, 0);
  if (err) {
    throw std::system_error(
        errno,
        std::system_category(),
        folly::sformat("failed to get ruh status, fd: {}", file_.fd()));
  }

  return buffer;
}

void FdpNvme::prepFdpUringCmdSqe(struct io_uring_sqe& sqe,
                                 void* buf,
                                 size_t size,
                                 off_t start,
                                 uint8_t opcode,
                                 uint8_t dtype,
                                 uint16_t dspec) {
  uint32_t maxTfrSize = nvmeData_.getMaxTfrSize();
  if ((maxTfrSize != 0) && (size > maxTfrSize)) {
    throw std::invalid_argument("Exceeds max Transfer size");
  }
  // Clear the SQE entry to avoid some arbitrary flags being set.
  memset(&sqe, 0, sizeof(struct io_uring_sqe));

  sqe.fd = file_.fd();
  sqe.opcode = IORING_OP_URING_CMD;
  sqe.cmd_op = NVME_URING_CMD_IO;

  struct nvme_uring_cmd* cmd = (struct nvme_uring_cmd*)&sqe.cmd;
  if (cmd == nullptr) {
    throw std::invalid_argument("Uring cmd is NULL!");
  }
  memset(cmd, 0, sizeof(struct nvme_uring_cmd));
  cmd->opcode = opcode;

  // start LBA of the IO = Req_start (offset in partition) + Partition_start
  uint64_t sLba = (start >> nvmeData_.lbaShift()) + nvmeData_.partStartLba();
  uint32_t nLb = (size >> nvmeData_.lbaShift()) - 1; // nLb is 0 based

  /* cdw10 and cdw11 represent starting lba */
  cmd->cdw10 = sLba & 0xffffffff;
  cmd->cdw11 = sLba >> 32;
  /* cdw12 represent number of lba's for read/write */
  cmd->cdw12 = (dtype & 0xFF) << 20 | nLb;
  cmd->cdw13 = (dspec << 16);
  cmd->addr = (uint64_t)buf;
  cmd->data_len = size;

  cmd->nsid = nvmeData_.nsId();
}

void FdpNvme::prepReadUringCmdSqe(struct io_uring_sqe& sqe,
                                  void* buf,
                                  size_t size,
                                  off_t start) {
  // Placement Handle is not used for read.
  prepFdpUringCmdSqe(sqe, buf, size, start, nvme_cmd_read, 0, 0);
}

void FdpNvme::prepWriteUringCmdSqe(
    struct io_uring_sqe& sqe, void* buf, size_t size, off_t start, int handle) {
  static constexpr uint8_t kPlacementMode = 2;
  uint16_t pid;

  if (handle == -1) {
    pid = getFdpPID(kDefaultPIDIdx); // Use the default stream
  } else if (handle >= 0 && handle <= maxPIDIdx_) {
    pid = getFdpPID(static_cast<uint16_t>(handle));
  } else {
    throw std::invalid_argument("Invalid placement identifier");
  }

  prepFdpUringCmdSqe(sqe, buf, size, start, nvme_cmd_write, kPlacementMode,
                     pid);
}

// Read the /sys/block/xx entry for any block device
std::string readDevAttr(const std::string& bName, const std::string& attr) {
  std::string path = "/sys/block/" + bName + '/' + attr;
  std::string entry;
  if (!folly::readFile(path.c_str(), entry)) {
    throw std::runtime_error(folly::sformat("Unable to read {}", path));
  }
  return entry;
}

// Get the Namespace ID of an NVMe block device
int getNvmeNsId(const std::string& nsName) {
  return (folly::to<int>(readDevAttr(nsName, "nsid")));
}

// Get the Max Transfer size in bytes for an NVMe block device
uint32_t getMaxTfrSize(const std::string& nsName) {
  // max_hw_sectors_kb : This is the maximum number of kilobytes supported in a
  // single data transfer.
  // (https://www.kernel.org/doc/Documentation/block/queue-sysfs.txt)
  return (1024u * /* multiply by kb */
          folly::to<uint32_t>(readDevAttr(nsName, "queue/max_hw_sectors_kb")));
}

// Get LBA shift of an NVMe block device
uint32_t getLbaShift(const std::string& nsName) {
  return folly::constexpr_log2(
      folly::to<uint32_t>(readDevAttr(nsName, "queue/logical_block_size")));
}

// Get the partition start in bytes
uint64_t getPartStart(const std::string& nsName, const std::string& partName) {
  return (512u * /* sysfs size is in terms of linux sector size */
          folly::to<uint64_t>(readDevAttr(nsName + "/" + partName, "start")));
}

// It returns nsName = "nvme0n1" for both "/dev/nvme0n1" and "/dev/nvme0n1p1".
// Also partName = "nvme0n1p1" for "/dev/nvme0n1p1" (partitioned NS),
// and  "" for "/dev/nvme0n1" (non-partitioned NS)
void getNsAndPartition(const std::string& bdevName,
                       std::string& nsName,
                       std::string& partName) {
  size_t lastSlashPos = bdevName.find_last_of('/');
  if (lastSlashPos == std::string::npos) {
    throw std::invalid_argument("Invalid block dev name");
  }

  std::string baseName = bdevName.substr(lastSlashPos + 1);
  size_t pPos = baseName.find_last_of('p');

  if (pPos == std::string::npos) {
    nsName = baseName;
    partName = {};
  } else {
    nsName = baseName.substr(0, pPos);
    partName = baseName;
  }
}

// Reads the NVMe related info from a valid NVMe device path
NvmeData FdpNvme::readNvmeInfo(const std::string& bdevName) {
  std::string nsName, partName;

  try {
    getNsAndPartition(bdevName, nsName, partName);
    int namespace_id = getNvmeNsId(nsName);
    uint32_t lbaShift = getLbaShift(nsName);
    uint32_t maxTfrSize = getMaxTfrSize(nsName);

    uint64_t startLba{0};
    if (!partName.empty()) {
      startLba = getPartStart(nsName, partName) >> lbaShift;
    }

    XLOGF(INFO,
          "Nvme Device Info, NS Id: {}, lbaShift: {},"
          " Max Transfer size: {}, start Lba: {}",
          namespace_id, lbaShift, maxTfrSize, startLba);

    return NvmeData{namespace_id, lbaShift, maxTfrSize, startLba};
  } catch (const std::system_error& e) {
    XLOGF(ERR, "Exception in readNvmeInfo for: {}", bdevName);
    throw;
  }
}

bool isValidNvmeDevice(const std::string& bdevName) {
  return std::regex_match(bdevName, std::regex("^/dev/nvme\\d+n\\d+(p\\d+)?$"));
}

// Converts an nvme block device name (ex: /dev/nvme0n1p1) to corresponding
// nvme char device name (ex: /dev/ng0n1), to use Nvme FDP directives.
std::string getNvmeCharDevice(const std::string& bdevName) {
  // Extract dev and NS IDs, and ignore partition ID.
  // Example: extract the string '0n1' from '/dev/nvme0n1p1'
  size_t devPos = bdevName.find_first_of("0123456789");
  size_t pPos = bdevName.find('p', devPos);

  return "/dev/ng" + bdevName.substr(devPos, pPos - devPos);
}

// Open Nvme Character device for the given block dev @bdevName.
// Throws std::system_error if failed.
folly::File FdpNvme::openNvmeCharFile(const std::string& bdevName) {
  if (!isValidNvmeDevice(bdevName)) {
    throw std::invalid_argument("Invalid NVMe device name");
  }

  int flags{O_RDONLY};
  folly::File f;

  try {
    auto cdevName = getNvmeCharDevice(bdevName);
    XLOGF(INFO, "Opening NVMe Char Dev file: {}", cdevName);
    f = folly::File(cdevName.c_str(), flags);
  } catch (const std::system_error& e) {
    XLOGF(ERR, "Exception in openNvmeCharFile for: {}", bdevName);
    throw;
  }
  XDCHECK_GE(f.fd(), 0);

  return f;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

#endif

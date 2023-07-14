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

#include <linux/nvme_ioctl.h>
#include <sys/ioctl.h>
#include <errno.h>
#include <cstring>
#include <numeric>
#include "cachelib/navy/common/NvmeInterface.h"

namespace facebook {
namespace cachelib {
namespace navy {

NvmeInterface::NvmeInterface(int fd) {
  XLOG(INFO)<< "Creating NvmeInterface, fd :" << fd;
  interface_ = createIOUringNvmeInterface();
  nvmeData_ = readNvmeInfo(fd);
}

int NvmeInterface::nvmeIOMgmtRecv(int fd, uint32_t nsid, void *data,
                  uint32_t data_len, uint16_t mos, uint8_t mo) {
  uint32_t cdw10 = (mo & 0xf) | (mos & 0xff << 16);
  uint32_t cdw11 = (data_len >> 2) - 1;

  struct nvme_passthru_cmd cmd = {
    .opcode             = nvme_cmd_io_mgmt_recv,
    .nsid               = nsid,
    .addr               = (uint64_t)(uintptr_t)data,
    .data_len           = data_len,
    .cdw10              = cdw10,
    .cdw11              = cdw11,
    .timeout_ms         = NVME_DEFAULT_IOCTL_TIMEOUT,
  };

  return ioctl(fd, NVME_IOCTL_IO_CMD, &cmd);
}

// struct nvme_fdp_ruh_status is a variable sized object; so using Buffer.
Buffer NvmeInterface::nvmeFdpStatus(int fd) {
  struct nvme_fdp_ruh_status hdr;
  int err;

  // Read FDP ruh status header to get Num_RUH Status Descriptors
  err = nvmeIOMgmtRecv(fd, nvmeData_.nsId(), &hdr, sizeof(hdr), 0,
      NVME_IO_MGMT_RECV_RUH_STATUS);
  if (err) {
    XLOG(ERR)<< "failed to get reclaim unit handle status header";
    return Buffer{};
  }

  auto size = sizeof(struct nvme_fdp_ruh_status) +
            (hdr.nruhsd * sizeof(struct nvme_fdp_ruh_status_desc));
  auto buffer = Buffer(size);

  // Read FDP RUH Status
  err = nvmeIOMgmtRecv(fd, nvmeData_.nsId(), buffer.data(), size, 0,
      NVME_IO_MGMT_RECV_RUH_STATUS);
  if (err) {
    XLOG(ERR)<< "failed to get reclaim unit handle status";
    return Buffer{};
  }

  return buffer;
}

IOData NvmeInterface::prepIO(int fd, uint64_t offset, uint32_t size,
                          const void* buf) {
  const uint8_t* bufp = reinterpret_cast<const uint8_t*>(buf);
  IOData ioData{fd, offset, size, const_cast<uint8_t*>(bufp)};
  return ioData;
}

bool NvmeInterface::doIO(int fd, uint64_t offset, uint32_t size,
                          const void* buf, InterfaceDDir dir) {
  uint32_t remainingSize = size;
  uint8_t* bufp = (uint8_t*)buf;
  uint32_t maxTfrSize = nvmeData_.maxTransferSize();
  bool ret;

  while (remainingSize) {
    auto ioSize = std::min<size_t>(maxTfrSize, remainingSize);
    IOData ioData = prepIO(fd, offset, ioSize, bufp);

    ret = interface_->nvmeIO(ioData, nvmeData_, dir);
    if(ret == false) {
      break;
    }
    offset += ioSize;
    bufp += ioSize;
    remainingSize -= ioSize;
  }
  return ret;
}

bool NvmeInterface::writeNvme(int fd, uint64_t offset, uint32_t size,
                          const void* buf) {
  return doIO(fd, offset, size, buf, DDIR_WRITE);
}

bool NvmeInterface::readNvme(int fd, uint64_t offset, uint32_t size,
                          void* buf) {
  return doIO(fd, offset, size, buf, DDIR_READ);
}

bool NvmeInterface::writeNvmeDirective(int fd, uint64_t offset, uint32_t size,
              const void* buf, uint16_t placementID) {
  // Dtype of 2 prompts the device to use data placement mode
  // And placementID is used as RG_RUH combination
  static constexpr uint8_t kPlacementMode = 2;
  uint32_t remainingSize = size;
  uint32_t maxTfrSize = nvmeData_.maxTransferSize();
  uint8_t* bufp = (uint8_t*)buf;
  bool ret;

  while (remainingSize) {
    auto ioSize = std::min<size_t>(maxTfrSize, remainingSize);
    IOData ioData = prepIO(fd, offset, ioSize, bufp);
    ioData.setDirectiveData(kPlacementMode, placementID);

    ret = interface_->nvmeIO(ioData, nvmeData_, DDIR_WRITE);
    if(ret == false) {
      break;
    }
    offset += ioSize;
    bufp += ioSize;
    remainingSize -= ioSize;
  }
  return ret;
}

NvmeData NvmeInterface::readNvmeInfo(int fd) {
  int namespace_id = ioctl(fd, NVME_IOCTL_ID);
  if (namespace_id < 0) {
    XLOG(ERR)<< "failed to fetch namespace-id, fd "<< fd;
    return NvmeData{};
  }

  char ctrl[NVME_IDENTIFY_DATA_SIZE]; // Identify ctrl data
  struct nvme_passthru_cmd cmd_ctrl = {
    .opcode         = nvme_admin_identify,
    .nsid           = 0,
    .addr           = (uint64_t)(uintptr_t)ctrl,
    .data_len       = NVME_IDENTIFY_DATA_SIZE,
    .cdw10          = NVME_IDENTIFY_CNS_CTRL,
    .cdw11          = NVME_CSI_NVM << NVME_IDENTIFY_CSI_SHIFT,
    .timeout_ms     = NVME_DEFAULT_IOCTL_TIMEOUT,
  };

  int err = ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd_ctrl);
  if(err) {
    XLOG(ERR)<< "failed to fetch identify ctrl";
    return NvmeData{};
  }

  static constexpr uint16_t kMDTSOffset = 77u;
  uint8_t mdts = (uint8_t)ctrl[kMDTSOffset];
  uint32_t maxTranferSize = (1 << mdts) * getpagesize();

  struct nvme_id_ns ns;
  struct nvme_passthru_cmd cmd_ns = {
    .opcode         = nvme_admin_identify,
    .nsid           = (uint32_t)namespace_id,
    .addr           = (uint64_t)(uintptr_t)&ns,
    .data_len       = NVME_IDENTIFY_DATA_SIZE,
    .cdw10          = NVME_IDENTIFY_CNS_NS,
    .cdw11          = NVME_CSI_NVM << NVME_IDENTIFY_CSI_SHIFT,
    .timeout_ms     = NVME_DEFAULT_IOCTL_TIMEOUT,
  };

  err = ioctl(fd, NVME_IOCTL_ADMIN_CMD, &cmd_ns);
  if(err) {
    XLOG(ERR)<< "failed to fetch identify namespace";
    return NvmeData{};
  }

  auto lbaShift = (uint32_t)ilog2(1 << ns.lbaf[(ns.flbas & 0x0f)].ds);
  XLOG(INFO) <<"Nvme Device Info :" <<namespace_id<<" lbashift: "
             <<lbaShift<<" size: "<<ns.nsze<<" max tfr size: "<<maxTranferSize;

  return NvmeData{namespace_id, lbaShift, ns.nsze, maxTranferSize};
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

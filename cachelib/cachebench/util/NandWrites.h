#pragma once
#include <folly/Range.h>
#include <folly/Subprocess.h>

#include <cstdint>

namespace facebook {
namespace hw {

// Process interface used to provide mock Process objects for unit tests.
class Process {
 public:
  virtual ~Process() {}

  // Read and write from the process stdin and stdout. The returned pair
  // is the process (stdout, stderr). See the caveat in folly/Subprocess.h
  // about deadlocks when using pipes for both stdin and stdout.
  virtual std::pair<std::string, std::string> communicate() = 0;

  virtual folly::ProcessReturnCode wait() = 0;
};

// Factory class used to create Process objects. This allows us to provide mock
// Process objects for unit tests.
class ProcessFactory {
 public:
  virtual ~ProcessFactory() {}
  virtual std::shared_ptr<Process> createProcess(
      const std::vector<std::string>& argv,
      const folly::Subprocess::Options& options,
      const char* executable = nullptr,
      const std::vector<std::string>* env = nullptr) const;
};

// Gets the lifetime physical NAND write total for a device.
//
// @param[in] deviceName       Identifier for a device. This is usually
//                             specified as nvme1n1 or nvme2n1 etc.
// @param[in] nvmePath         Path to the `nvme` tool. Most users should leave
//                             this set to the default value.
// @param[in] processFactory   Interface used to spawn subprocesses, used to
//                             inject a mock for unit tests.
//
// @returns Lifetime total physical (NAND) bytes written to the device.
//
// @throws std::runtime_error if the device is not recognized or an error occurs
//         when running the `nvme` command.
uint64_t nandWriteBytes(const folly::StringPiece& deviceName,
                        const folly::StringPiece& nvmePath = "/usr/sbin/nvme",
                        std::shared_ptr<ProcessFactory> processFactory =
                            std::make_shared<ProcessFactory>());

} // namespace hw
} // namespace facebook

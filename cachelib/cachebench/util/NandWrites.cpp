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

#include "cachelib/cachebench/util/NandWrites.h"

#include <folly/Format.h>
#include <folly/String.h>
#include <folly/Subprocess.h>
#include <folly/json.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <string>
#include <vector>

namespace facebook {
namespace hw {

// Simple wrapper around folly::Subprocess.
class SubprocessWrapper : public Process {
 public:
  SubprocessWrapper(const std::vector<std::string>& argv,
                    const folly::Subprocess::Options& options,
                    const char* executable = nullptr,
                    const std::vector<std::string>* env = nullptr)
      : subprocess_(argv, options, executable, env) {}

  virtual ~SubprocessWrapper() {}

  virtual std::pair<std::string, std::string> communicate() {
    return subprocess_.communicate();
  }

  virtual folly::ProcessReturnCode wait() { return subprocess_.wait(); }

 private:
  folly::Subprocess subprocess_;
};

std::shared_ptr<Process> ProcessFactory::createProcess(
    const std::vector<std::string>& argv,
    const folly::Subprocess::Options& options,
    const char* executable,
    const std::vector<std::string>* env) const {
  return std::make_shared<SubprocessWrapper>(argv, options, executable, env);
}

namespace {

void printCmd(const std::vector<std::string>& argv) {
  auto s =
      std::accumulate(argv.begin(),
                      argv.end(),
                      std::string(""),
                      [](const auto& a, const auto& b) { return a + " " + b; });
  XLOG(DBG) << "Running command: " << s;
}

bool runNvmeCmd(const std::shared_ptr<ProcessFactory>& processFactory,
                const folly::StringPiece& nvmePath,
                const std::vector<std::string>& args,
                std::string& out) {
  std::vector<std::string> argv{nvmePath.str()};
  std::copy(args.begin(), args.end(), std::back_inserter(argv));
  printCmd(argv);

  // Note that we can't just provide a command line here since then Subprocess
  // will use the shell, and this code cannot use the shell since it is invoked
  // by a setuid root binary in some contexts.
  try {
    auto proc =
        processFactory->createProcess(argv,
                                      folly::Subprocess::Options().pipeStdout(),
                                      nvmePath.data(),
                                      nullptr /* env */);
    XDCHECK(proc);
    const auto& [stdout, stderr] = proc->communicate();
    const auto& rc = proc->wait();
    bool success = rc.exitStatus() == 0;
    if (success) {
      XLOG(DBG) << "Got output: " << stdout;
      out = stdout;
    }
    return success;
  } catch (const folly::SubprocessSpawnError& e) {
    XLOG(ERR) << e.what();
    return false;
  }
}

// Get the "bytes written" line in the `nvme` output for a device, as a vector
// of space-delimited fields.
//
// Runs `nvme` with the given arguments, looks for the first line containing
// the string "ritten", and then extracts and returns the given (space
// delimited) fields. If no matching line is found, returns an empty vector.
std::vector<std::string> getBytesWrittenLine(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece& nvmePath,
    const std::vector<std::string>& args) {
  std::string out;
  if (!runNvmeCmd(processFactory, nvmePath, args, out)) {
    XLOG(ERR) << "Failed to run nvme command!";
    return {};
  }

  // The existing NandWrite code expects a line that matches the pattern
  // /ritten/, so that's what we do here. We just use the first matching
  // line.
  std::vector<folly::StringPiece> lines;
  folly::split('\n', out, lines, true /* ignoreEmpty */);
  for (const auto& line : lines) {
    if (line.find("ritten") != std::string::npos) {
      std::vector<std::string> fields;
      folly::split(' ', line, fields, true /* ignoreEmpty */);
      return fields;
    }
  }

  XLOG(ERR) << "No matching line found in nvme output!";
  return {};
}

// Get the "write count" for a drive using `nvme`. Note that different vendors
// use different units for this value, hence the "factor" argument; see the
// functions below.
std::optional<uint64_t> getBytesWritten(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece& nvmePath,
    const std::vector<std::string>& args,
    const size_t fieldNum,
    const uint64_t factor) {
  std::vector<std::string> fields =
      getBytesWrittenLine(processFactory, nvmePath, args);
  XLOG(DBG) << "got fields: " << folly::join(",", fields);
  if (fields.size() <= fieldNum) {
    XLOG(ERR) << "Unexpected number of fields in line! Got " << fields.size()
              << " fields, but expected at least " << fieldNum + 1 << ".";
    return std::nullopt;
  }
  fields[fieldNum].erase(
      std::remove(fields[fieldNum].begin(), fields[fieldNum].end(), ','),
      fields[fieldNum].end());
  return std::stoll(fields[fieldNum], 0 /* pos */, 0 /* base */) * factor;
}

// The output for a Samsung device looks like:
//
// clang-format off
// ...
// [015:000] PhysicallyWrittenBytes                            : 5357954930143232
// ...
// clang-format on
//
// Note that Samsung supports JSON output, but for simplicity we parse
// all the vendor output the same way.
std::optional<uint64_t> samsungWriteBytes(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece& nvmePath,
    const folly::StringPiece& devicePath) {
  // For Samsung devices, the returned count is already in bytes.
  return getBytesWritten(processFactory,
                         nvmePath,
                         {"samsung", "vs-smart-add-log", devicePath.str()},
                         3 /* field num */,
                         1 /* factor */);
}

// The output for a LiteOn device looks like:
//
// clang-format off
// ...
// Physical(NAND) bytes written                  : 157,035,510,104,064
// ...
// clang-format on
std::optional<uint64_t> liteonWriteBytes(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece& nvmePath,
    const folly::StringPiece& devicePath) {
  // For LiteOn devices, the returned count is already in bytes.
  return getBytesWritten(processFactory,
                         nvmePath,
                         {"liteon", "vs-smart-add-log", devicePath.str()},
                         4 /* field num */,
                         1 /* factor */);
}

// The output for a Seagate device looks like this:
//
// clang-format off
// Seagate Extended SMART Information :
// Description                             Ext-Smart-Id    Ext-Smart-Value
// --------------------------------------------------------------------------------
// ...
// Physical (NAND) bytes written           60137           0x000000000000000000000002d0cdc01b
// ...
// clang-format on
std::optional<uint64_t> seagateWriteBytes(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece& nvmePath,
    const folly::StringPiece& devicePath) {
  // For Segate, the output is a count of 500 KiB blocks written.
  //
  // XXX The code in NandWrites.cpp assumes this, but the name of the attribute
  // in the output is "bytes written" -- so which is it?
  return getBytesWritten(processFactory,
                         nvmePath,
                         {"seagate", "vs-smart-add-log", devicePath.str()},
                         5 /* field num */,
                         500 * 1024 /* factor */);
}

// The output for a Toshiba device looks like:
//
// clang-format off
// Vendor Log Page 0xCA for NVME device:nvme0 namespace-id:ffffffff
// Total data written to NAND               : 1133997.9 GiB
// ...
// clang-format on
std::optional<uint64_t> toshibaWriteBytes(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece& nvmePath,
    const folly::StringPiece& devicePath) {
  // We expect the units to be one of 'KiB', 'MiB', 'GiB', or 'TiB'.
  const auto& fields =
      getBytesWrittenLine(processFactory,
                          nvmePath,
                          {"toshiba", "vs-smart-add-log", devicePath.str()});
  // There should be 8 fields in the "data written" line.
  constexpr size_t kExpectedFieldCount = 8;
  if (fields.size() != kExpectedFieldCount) {
    XLOG(ERR) << "Wrong number of fields! Got " << fields.size()
              << " fields, but expected exactly " << kExpectedFieldCount << ".";
    return std::nullopt;
  }

  const auto& value = std::stoll(fields[6]);
  const auto& units = fields[7];
  if (units == "KiB") {
    return value * 1024;
  } else if (units == "MiB") {
    return value * 1024 * 1024;
  } else if (units == "GiB") {
    return value * 1024 * 1024 * 1024;
  } else if (units == "TiB") {
    return value * 1024 * 1024 * 1024 * 1024;
  }

  XLOG(ERR) << "Unrecognized units " << units << " in nvme output!";
  return std::nullopt;
}

// The output for an Intel device looks like:
//
// clang-format off
// Additional Smart Log for NVME device:nvme0 namespace-id:ffffffff
// key                               normalized raw
// ...
// nand_bytes_written              : 100%       sectors: 224943088
// ...
// clang-format on
//
// Note that Intel supports JSON output, but for simplicity we parse
// all the vendor output the same way.
std::optional<uint64_t> intelWriteBytes(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece& nvmePath,
    const folly::StringPiece& devicePath) {
  // For Intel devices, the output is in number of 32 MB pages.
  //
  // XXX The code in NandWrites assumes that the output is a number of 32 MB
  // pages, but the actual `nvme` output for an Intel device says "sectors" and
  // the `nvme list` output lists a sector size of 4 KiB.
  return getBytesWritten(processFactory,
                         nvmePath,
                         {"intel", "smart-log-add", devicePath.str()},
                         4 /* field num */,
                         32 * 1024 * 1024 /* factor */);
}

// The output for an WDC device looks like:
//
// clang-format off
// Additional Smart Log for NVME device:nvme0 namespace-id:ffffffff
// key                               normalized raw
// ...
// Physical media units written -   	        0 2068700589752320
// ...
// clang-format on
//
std::optional<uint64_t> wdcWriteBytes(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece nvmePath,
    const folly::StringPiece devicePath) {
  // For WDC devices, the output is in number of bytes.
  //
  return getBytesWritten(processFactory,
                         nvmePath,
                         {"wdc", "vs-smart-add-log", devicePath.str()},
                         7 /* field num */,
                         1 /* factor */);
}

// The output for a Micron device looks like:
//
// clang-format off
// Additional Smart Log for NVME device:nvme0 namespace-id:ffffffff
// key                                      : raw
// ...
// Physical Media Units Written             : 0xd848690000
// ...
// clang-format on
//
std::optional<uint64_t> micronWriteBytes(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece nvmePath,
    const folly::StringPiece devicePath) {
  // For Micron devices, the output is in number of bytes hex format.
  //
  return getBytesWritten(
      processFactory,
      nvmePath,
      {"micron", "vs-smart-add-log", devicePath.str(), "-f normal"},
      6 /* field num */,
      1 /* factor */);
}

std::optional<uint64_t> skhmsWriteBytes(
    const std::shared_ptr<ProcessFactory>& processFactory,
    const folly::StringPiece nvmePath,
    const folly::StringPiece devicePath) {
  return getBytesWritten(processFactory,
                         nvmePath,
                         {"skhms", "vs-nand-stats", devicePath.str()},
                         7 /* field num */,
                         1 /* factor */);
}

// Gets the output of `nvme list` for the given device.
std::optional<std::string> getDeviceModelNumber(
    std::shared_ptr<ProcessFactory> processFactory,
    const folly::StringPiece& nvmePath,
    const folly::StringPiece& devicePath) {
  std::string out;
  if (!runNvmeCmd(processFactory, nvmePath, {"list", "-o", "json"}, out)) {
    XLOG(ERR) << "Failed to run nvme command!";
    return std::nullopt;
  }

  try {
    const auto& obj = folly::parseJson(out);
    const auto& devices = obj["Devices"];
    for (const auto& device : devices) {
      XLOG(DBG) << "Considering device " << device["DevicePath"].asString();
      if (device["DevicePath"].asString() == devicePath) {
        XLOG(DBG) << "Device matched, returning model number "
                  << device["ModelNumber"].asString();
        return device["ModelNumber"].asString();
      }
    }
  } catch (const folly::json::parse_error& e) {
    XLOG(ERR) << e.what();
  }

  // No matching device in the nvme output.
  return std::nullopt;
}

} // anonymous namespace

// TODO: add unit tests
uint64_t nandWriteBytes(const folly::StringPiece& deviceName,
                        const folly::StringPiece& nvmePath,
                        std::shared_ptr<ProcessFactory> processFactory) {
  const auto& devicePath = folly::sformat("/dev/{}", deviceName);
  auto modelNumber = getDeviceModelNumber(processFactory, nvmePath, devicePath);
  if (!modelNumber) {
    throw std::invalid_argument(
        folly::sformat("Failed to get device info for device {}", deviceName));
  }
  folly::toLowerAscii(modelNumber.value());

  static const std::map<std::string,
                        std::function<std::optional<uint64_t>(
                            const std::shared_ptr<ProcessFactory>&,
                            const folly::StringPiece&,
                            const folly::StringPiece&)>>
      vendorMap{{"samsung", samsungWriteBytes},
                {"mz1lb960hbjr-", samsungWriteBytes},
                {"mzol23t8hcls-", samsungWriteBytes},
                // The Samsung PM983a doesn't include Samsung in the model
                // number at this time, but it's a Samsung device.
                {"liteon", liteonWriteBytes},
                {"ssstc", liteonWriteBytes},
                {"intel", intelWriteBytes},
                {"seagate", seagateWriteBytes},
                // The Seagate XM1441 doesn't include SEAGATE in the model
                // number, but it's a Segate device.
                {"xm1441-", seagateWriteBytes},
                {"toshiba", toshibaWriteBytes},
                {"wus4bb019d4m9e7", wdcWriteBytes},
                {"wus4bb019djese7", wdcWriteBytes},
                {"wus4bb038djese7", wdcWriteBytes},
                {"micron", micronWriteBytes},
                {"hfs512gde9x083n", skhmsWriteBytes}};
  for (const auto& [vendor, func] : vendorMap) {
    XLOG(DBG) << "Looking for vendor " << vendor << " in device model string \""
              << modelNumber.value() << "\".";
    if (modelNumber.value().find(vendor) != std::string::npos) {
      XLOG(DBG) << "Matched vendor " << vendor;
      const auto& bytesWritten = func(processFactory, nvmePath, devicePath);
      if (!bytesWritten) {
        // Throw an exception to maintain the same contract as the old version
        // of this code.
        //
        // TODO: update the method to return an optional instead?
        throw std::invalid_argument(folly::sformat(
            "Failed to get bytes written for device {}", deviceName));
      }
      return bytesWritten.value();
    }
  }

  // We got a model string but didn't match the vendor.
  throw std::invalid_argument(folly::sformat(
      "Vendor not recogized in device model number {}", modelNumber.value()));
}

} // namespace hw
} // namespace facebook

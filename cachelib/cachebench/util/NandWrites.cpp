#include "cachelib/cachebench/util/NandWrites.h"

#include <algorithm>
#include <cctype>
#include <cstdio>
#include <string>

#include <folly/Format.h>

namespace facebook {
namespace hw {

namespace {

constexpr size_t MB = 1024ULL * 1024ULL;

std::string runCommand(std::string cmd) {
  std::array<char, 128> buffer;
  std::string output = "";

  FILE* pipe = popen(cmd.c_str(), "r");
  if (!pipe) {
    return "Cannot open!";
  }
  while (!feof(pipe)) {
    if (fgets(buffer.data(), buffer.size(), pipe) != nullptr) {
      output += buffer.data();
    }
  }
  pclose(pipe);
  return output;
}

uint64_t samsungWriteBytes(folly::StringPiece device) {
  auto cmd = folly::sformat(
      "nvme samsung vs-smart-add-log /dev/{} | grep ritten | awk '{{ print "
      "$4 }}'",
      device);
  std::string result = runCommand(cmd);
  result.erase(std::remove(result.begin(), result.end(), ','), result.end());
  result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
  uint64_t nWrites = std::stoll(result);
  // output is in bytes
  return nWrites;
}

uint64_t liteonWriteBytes(folly::StringPiece device) {
  auto cmd = folly::sformat(
      "nvme liteon vs-smart-add-log /dev/{} | grep ritten | awk '{{ print $5 "
      "}}'",
      device);
  std::string result = runCommand(cmd);
  result.erase(std::remove(result.begin(), result.end(), ','), result.end());
  result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
  uint64_t nWrites = std::stoll(result);
  // output is in bytes
  return nWrites;
}

uint64_t intelWriteBytes(folly::StringPiece device) {
  auto cmd = folly::sformat(
      "nvme intel smart-log-add /dev/{} | grep ritten | awk '{{ print $5}}'",
      device);
  std::string result = runCommand(cmd);
  result.erase(std::remove(result.begin(), result.end(), ','), result.end());
  result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
  uint64_t nWrites = std::stoll(result);
  // output is in number of 32MB pages
  return nWrites * 32 * MB;
}
uint64_t seagateWriteBytes(folly::StringPiece device) {
  auto cmd = folly::sformat(
      "nvme seagate vs-smart-add-log /dev/{} | grep ritten | awk '{{print "
      "$5}}'",
      device);
  std::string result = runCommand(cmd);
  result.erase(std::remove(result.begin(), result.end(), ','), result.end());
  result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
  uint64_t nWrites = std::stoll(result);
  return (nWrites / (1024 / 500)) * MB;
}

uint64_t skhmsWriteBytes(folly::StringPiece device) {
  auto cmd = folly::sformat(
      "nvme skhms vs-skhms-smart-log /dev/{} | grep ritten | awk '{{print "
      "$5}}'",
      device);
  std::string result = runCommand(cmd);
  result.erase(std::remove(result.begin(), result.end(), ','), result.end());
  result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
  uint64_t nWrites = std::stoll(result);
  // output is in 512 byte pages
  return nWrites * 512;
}

uint64_t toshibaWriteBytes(folly::StringPiece device) {
  auto cmd = folly::sformat(
      "nvme toshiba vs-smart-add-log /dev/{} | grep ritten | awk '{{ print "
      "$7}}'",
      device);
  std::string result = runCommand(cmd);
  result.erase(std::remove(result.begin(), result.end(), ','), result.end());
  result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
  uint64_t nWrites = std::stoll(result);
  // output is in KB
  return (nWrites * 1024);
}

uint64_t wdcWriteBytes(folly::StringPiece device) {
  auto cmd = folly::sformat(
      "nvme wdc smart-add-log /dev/{} | grep ritten | awk '{{ print $5 }}'",
      device);
  std::string result = runCommand(cmd);
  result.erase(std::remove(result.begin(), result.end(), ','), result.end());
  result.erase(std::remove(result.begin(), result.end(), '\n'), result.end());
  uint64_t nWrites = std::stol(result);
  // output is in 512 byte pages
  return nWrites * 512;
}

std::string getVendorName(folly::StringPiece device, size_t columnNum) {
  std::string cmd = folly::sformat(
      "nvme list | grep {} | awk '{{ print ${} }}'", device, columnNum);
  auto s = runCommand(cmd);
  std::transform(s.begin(), s.end(), s.begin(), ::tolower);
  s.erase(std::remove(s.begin(), s.end(), '\n'), s.end());
  return s;
}

bool isValidDevice(folly::StringPiece device) {
  for (auto c : device) {
    // lower case alphanum is the valid combination
    if (!std::isalnum(c) || (std::isalpha(c) && !std::islower(c))) {
      return false;
    }
  }
  return !device.empty();
}

} // namespace

uint64_t nandWriteBytes(folly::StringPiece device) {
  if (!isValidDevice(device)) {
    throw std::invalid_argument(folly::sformat("Invalid device {}", device));
  }

  const std::string vendorA = getVendorName(device, 3);
  const std::string vendorB = getVendorName(device, 4);
  if (vendorA == "liteon" || vendorB == "liteon") {
    return liteonWriteBytes(device);
  } else if (vendorA == "samsung" || vendorB == "samsung") {
    return samsungWriteBytes(device);
  } else if (vendorA == "toshiba" || vendorB == "toshiba") {
    return toshibaWriteBytes(device);
  } else if (vendorA == "intel" || vendorB == "intel") {
    return intelWriteBytes(device);
  } else if (vendorA == "seagate" || vendorB == "seagate") {
    return seagateWriteBytes(device);
  } else if (vendorA == "wdc" || vendorB == "wdc") {
    return wdcWriteBytes(device);
  } else if (vendorA == "skhms" || vendorB == "skhms") {
    return skhmsWriteBytes(device);
  } else if (vendorA.empty() && vendorB.empty()) {
    throw std::invalid_argument(
        folly::sformat("Can not find information for device {}", device));

  } else {
    throw std::invalid_argument(
        folly::sformat("Vendor {} {} not recognized", vendorA, vendorB));
  }
}

} // namespace hw
} // namespace facebook

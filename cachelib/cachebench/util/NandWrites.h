#pragma once
#include <cstdint>

#include <folly/Range.h>

namespace facebook {
namespace hw {

// obtains the physical nand writes cumulative counter from the corresponding
// firmware hooks.
//
// @param  device   Identifier for a device. This is usually specified as
//                  nvme1n1 or nvme2n1 etc.
// @return  bytes erased at nand level
// @throw   exception if the device is not recognized with the supported nvme
//          tool chain or if the credentials of the caller are not sufficient
//          for invoking the nvme tool chain
uint64_t nandWriteBytes(folly::StringPiece device);

} // namespace hw
} // namespace facebook

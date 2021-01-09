#include <folly/Format.h>
#include <folly/Range.h>

#include <iostream>

#include "cachelib/cachebench/util/NandWrites.h"

int main(int argc, char** argv) {
  folly::StringPiece helpStr =
      "Usage {} [device]\n\n\nObtains device write info using nvme tool chain. "
      "Might need root priveleges.\nDevice should be specified without the "
      "prefixing /dev. For example nvme0n1";
  if (argc != 2) {
    std::cerr << folly::format(helpStr, argv[0]) << std::endl;
    return -22;
  }

  folly::StringPiece device{argv[1]};
  try {
    std::cout << "Fetching info for device " << device << std::endl;
    auto val = facebook::hw::nandWriteBytes(device);
    std::cout << "Nand writes = " << val << std::endl;
  } catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    return 1;
  }
  return 0;
}

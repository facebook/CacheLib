#include "BufferGen.h"

namespace facebook {
namespace cachelib {
namespace navy {
const char BufferGen::kAlphabet[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "abcdefghijklmnopqrstuvwxyz"
    "0123456789=+";

Buffer BufferGen::gen(uint32_t size) {
  Buffer buf{size};
  auto p = buf.data();
  for (uint32_t i = 0; i < size; i++) {
    p[i] = static_cast<uint8_t>(kAlphabet[rg_() % (sizeof(kAlphabet) - 1)]);
  }
  return buf;
}

Buffer BufferGen::gen(uint32_t sizeMin, uint32_t sizeMax) {
  if (sizeMin == sizeMax) {
    return gen(sizeMin);
  } else {
    return gen(sizeMin + static_cast<uint32_t>(rg_() % (sizeMax - sizeMin)));
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

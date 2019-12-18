#include "Buffer.h"

#include <algorithm>
#include <cstdio>
#include <cstring>

namespace facebook {
namespace cachelib {
namespace navy {
namespace {
bool isLikeText(BufferView view) {
  for (size_t i = 0; i < view.size(); i++) {
    if (!between(view.byteAt(i), 32, 127)) {
      return false;
    }
  }
  return true;
}
} // namespace

std::string toString(BufferView view, bool compact) {
  std::string rv;
  if (isLikeText(view)) {
    rv.append("BufferView \"")
        .append(reinterpret_cast<const char*>(view.data()), view.size())
        .append("\"");
  } else {
    rv.append("BufferView size=")
        .append(std::to_string(view.size()))
        .append(" <");
    char buf[16]{};
    auto maxVisible = compact ? std::min(view.size(), size_t{80}) : view.size();
    for (size_t i = 0; i < maxVisible; i++) {
      std::snprintf(buf, sizeof(buf), "%02x", view.byteAt(i));
      rv.append(buf);
    }
    if (view.size() > maxVisible) {
      rv.append("...");
    }
    rv.append(">");
  }
  return rv;
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

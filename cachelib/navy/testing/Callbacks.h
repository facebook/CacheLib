#pragma once

#include <folly/Range.h>
#include <gmock/gmock.h>

#include "cachelib/navy/AbstractCache.h"
#include "cachelib/navy/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {
namespace navy {
struct MockDestructor {
  MOCK_METHOD3(call, void(BufferView, BufferView, DestructorEvent));
};

struct MockCounterVisitor {
  MOCK_METHOD2(call, void(folly::StringPiece, double));
};

struct MockInsertCB {
  MOCK_METHOD2(call, void(Status, BufferView));
};

struct MockLookupCB {
  MOCK_METHOD3(call, void(Status, BufferView, BufferView));
};

struct MockRemoveCB {
  MOCK_METHOD2(call, void(Status, BufferView));
};

template <typename MockCB>
auto toCallback(MockCB& mock) {
  return bindThis(&MockCB::call, mock);
}

inline folly::StringPiece strPiece(const char* strz) { return strz; }
} // namespace navy
} // namespace cachelib
} // namespace facebook

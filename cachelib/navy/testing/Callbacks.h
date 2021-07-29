#pragma once

#include <folly/Range.h>
#include <gmock/gmock.h>

#include "cachelib/navy/AbstractCache.h"
#include "cachelib/navy/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {
namespace navy {
//
// The following classes are mock classes that are meant for unit tests. They
// provide a call() function which user can use to specify various expected
// scenarios expected in unit tests. To use them with Navy callbacks, user
// should create a mock object first, and then call toCallback() to convert
// them to function objects expected by Navy.
//

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

// Helper utility that converts a c-style string to a StringPiece
inline folly::StringPiece strPiece(const char* strz) { return strz; }
} // namespace navy
} // namespace cachelib
} // namespace facebook

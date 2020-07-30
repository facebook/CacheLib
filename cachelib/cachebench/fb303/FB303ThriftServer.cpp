#include "cachelib/cachebench/fb303/FB303ThriftServer.h"

#include <folly/logging/xlog.h>

namespace facebook {
namespace cachelib {
namespace cachebench {
FB303ThriftService::FB303ThriftService(int port)
    : service_("CacheBench FB303 Service") {
  if (port <= 0) {
    return;
  }
  auto server = std::make_shared<apache::thrift::ThriftServer>();
  try {
    auto handler = std::make_shared<FB303ThriftHandler>();
    server->setInterface(handler);
    server->setPort(static_cast<uint16_t>(port));
    service_.addThriftService(server, handler.get(), port);
    service_.go(false /* don't wait */);
    started_ = true;
  } catch (...) {
    XLOG(ERR) << "Failed to setup fb303 thrift server";
    throw;
  }
}

FB303ThriftService::~FB303ThriftService() {
  if (started_) {
    service_.stop();
    service_.waitForStop();
  }
}
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

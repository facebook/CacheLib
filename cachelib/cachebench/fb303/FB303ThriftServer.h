#pragma once

#include "cachelib/cachebench/fb303/if/gen-cpp2/FB303Service.h"
#include "common/fb303/cpp/FacebookBase2.h"
#include "common/services/cpp/ServiceFramework.h"
#include "thrift/lib/cpp2/server/ThriftServer.h"

namespace facebook {
namespace cachelib {
namespace cachebench {
class FB303ThriftHandler : public FB303ServiceSvIf,
                           public fb303::FacebookBase2 {
 public:
  FB303ThriftHandler() : FacebookBase2("CacheBench FB303 Service") {}
  fb303::cpp2::fb_status getStatus() override {
    return fb303::cpp2::fb_status::ALIVE;
  }
};

class FB303ThriftService {
 public:
  explicit FB303ThriftService(int port);

  ~FB303ThriftService();

 private:
  bool started_{false};
  facebook::services::ServiceFramework service_;
};
} // namespace cachebench
} // namespace cachelib
} // namespace facebook

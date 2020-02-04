#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {
const char* toString(Status status) {
  switch (status) {
  case Status::Ok:
    return "Ok";
  case Status::NotFound:
    return "NotFound";
  case Status::Rejected:
    return "Rejected";
  case Status::Retry:
    return "Retry";
  case Status::DeviceError:
    return "DeviceError";
  case Status::BadState:
    return "BadState";
  }
  return "Unknown";
}

const char* toString(DestructorEvent e) {
  switch (e) {
  case DestructorEvent::Recycled:
    return "Recycled";
  case DestructorEvent::Removed:
    return "Removed";
  }
  return "Unknown";
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

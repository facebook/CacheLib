
#include <folly/Format.h>
#include <folly/Random.h>
#include <folly/logging/xlog.h>

#include "cachelib/navy/admission_policy/RejectRandomAP.h"
#include "cachelib/navy/common/Utils.h"

namespace facebook {
namespace cachelib {
namespace navy {
RejectRandomAP::Config& RejectRandomAP::Config::validate() {
  if (!between(probability, 0, 1)) {
    throw std::invalid_argument{
        folly::sformat("probability out of [0, 1] range: {}", probability)};
  }
  return *this;
}

RejectRandomAP::RejectRandomAP(Config&& config, ValidConfigTag)
    : probability_{config.probability}, rg_{config.seed} {
  XLOGF(INFO, "RejectRandomAP: probability {}", probability_);
}

bool RejectRandomAP::accept(HashedKey /* hk */, BufferView /* value */) {
  if (probability_ == 1) {
    // Code in the "else" block doesn't produce correct results for
    // probability 1. Return true explicitly.
    return true;
  } else {
    return fdiv(rg_(), rg_.max()) < probability_;
  }
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

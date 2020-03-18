#include "cachelib/navy/testing/MockDevice.h"

namespace facebook {
namespace cachelib {
namespace navy {
MockDevice::MockDevice(uint32_t deviceSize,
                       uint32_t blockSize,
                       std::shared_ptr<DeviceEncryptor> encryptor)
    : blockSize_{blockSize},
      device_{deviceSize == 0
                  ? nullptr
                  : createMemoryDevice(deviceSize, std::move(encryptor))} {
  ON_CALL(*this, readImpl(testing::_, testing::_, testing::_))
      .WillByDefault(
          testing::Invoke([this](uint64_t offset, uint32_t size, void* buffer) {
            XDCHECK_EQ(size % blockSize_, 0u);
            XDCHECK_EQ(offset % blockSize_, 0u);
            return device_->read(offset, size, buffer);
          }));

  ON_CALL(*this, writeImpl(testing::_, testing::_, testing::_))
      .WillByDefault(testing::Invoke(
          [this](uint64_t offset, uint32_t size, const void* buffer) {
            XDCHECK_EQ(size % blockSize_, 0u);
            XDCHECK_EQ(offset % blockSize_, 0u);
            return device_->write(offset, size, buffer);
          }));

  ON_CALL(*this, flushImpl()).WillByDefault(testing::Invoke([this]() {
    device_->flush();
  }));
}
} // namespace navy
} // namespace cachelib
} // namespace facebook

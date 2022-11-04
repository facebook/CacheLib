/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cachelib/navy/serialization/RecordIO.h"

#include <folly/Format.h>
#include <folly/Range.h>
#include <folly/io/RecordIO.h>

using namespace folly::recordio_helpers;

namespace facebook {
namespace cachelib {
namespace navy {
constexpr uint32_t kMetadataHeaderFileId = 1;
namespace {
class FileRecordWriter final : public RecordWriter {
 public:
  explicit FileRecordWriter(int fd) : writer_{folly::File(fd)} {}
  explicit FileRecordWriter(folly::File file) : writer_(std::move(file)) {}
  ~FileRecordWriter() override = default;

  void writeRecord(std::unique_ptr<folly::IOBuf> buf) override {
    writer_.write(std::move(buf));
  }
  bool invalidate() override { return false; }

 private:
  folly::RecordIOWriter writer_;
};

class FileRecordReader final : public RecordReader {
 public:
  explicit FileRecordReader(int fd)
      : reader_{folly::File(fd)}, curr_{reader_.seek(0)} {}
  explicit FileRecordReader(folly::File file)
      : reader_{std::move(file)}, curr_{reader_.seek(0)} {}
  ~FileRecordReader() override = default;

  std::unique_ptr<folly::IOBuf> readRecord() override {
    auto buf = folly::IOBuf::copyBuffer(curr_->first);
    ++curr_;
    return buf;
  }

  bool isEnd() const override { return curr_ == reader_.end(); }

 private:
  folly::RecordIOReader reader_;
  folly::RecordIOReader::Iterator curr_;
};

class DeviceMetaDataWriter final : public RecordWriter {
 public:
  explicit DeviceMetaDataWriter(Device& dev, size_t metadataSize)
      : dev_(dev),
        metadataSize_{metadataSize},
        blockSize_{dev_.getIOAlignmentSize() >= kBlockSizeDefault
                       ? dev_.getIOAlignmentSize()
                       : kBlockSizeDefault} {}

  ~DeviceMetaDataWriter() override {
    uint8_t* bufferData = buffer_.data();
    // Write the last remaining bytes to the device
    if (bufIndex_ > 0) {
      if (offset_ + blockSize_ < metadataSize_) {
        Buffer buffer = dev_.makeIOBuffer(blockSize_);
        memcpy(buffer.data(), bufferData, bufIndex_);
        memset(buffer.data() + bufIndex_, 0, blockSize_ - bufIndex_);
        dev_.write(offset_, std::move(buffer));
        offset_ += blockSize_;
      }
    }
    if (offset_ + blockSize_ <= metadataSize_) {
      // Write an additional block of zeroed out memory just to make the end
      // of metadata clear
      Buffer buffer = dev_.makeIOBuffer(blockSize_);
      memset(buffer.data(), 0, blockSize_);
      dev_.write(offset_, std::move(buffer));
    }
  }

  void writeRecord(std::unique_ptr<folly::IOBuf> buf) override {
    size_t totalLength = prependHeader(buf, kMetadataHeaderFileId);
    if (totalLength == 0) {
      return;
    }

    buf->unshare();
    buf->coalesce();
    auto size = buf->length();
    auto data = buf->data();
    auto dataOffset = 0;
    uint8_t* bufferData = buffer_.data();

    auto flushBuffer = [=]() mutable {
      auto extraBytes = blockSize_ - bufIndex_;
      // zero the unused bytes in the buffer
      memset(&bufferData[bufIndex_], 0, extraBytes);
      Buffer buffer = dev_.makeIOBuffer(blockSize_);
      memcpy(buffer.data(), bufferData, blockSize_);

      if (!dev_.write(offset_, std::move(buffer))) {
        throw std::invalid_argument(
            folly::sformat("write failed: offset = {}", offset_));
      }

      offset_ += blockSize_;
      bufIndex_ = 0;
    };

    do {
      // flush current buffer if it has no room for header
      if (bufIndex_ + headerSize() > blockSize_) {
        flushBuffer();
      }

      // if current input data total size is larger than capped size,
      // flush current buffer and raise exception.
      if (offset_ + bufIndex_ + size > metadataSize_) {
        if (bufIndex_ != 0) {
          flushBuffer();
        }
        throw std::logic_error("exceeding metadata limit");
      }

      // fill local buffer with input data
      auto cpBytes = std::min(static_cast<uint64_t>(blockSize_ - bufIndex_),
                              static_cast<uint64_t>(size));
      memcpy(&bufferData[bufIndex_], data + dataOffset, cpBytes);
      dataOffset += cpBytes;
      bufIndex_ += cpBytes;
      size -= cpBytes;
    } while (size > 0);
  }

  bool invalidate() override {
    Buffer invalidateBuffer{blockSize_, blockSize_};
    memset(invalidateBuffer.data(), 0, blockSize_);
    return dev_.write(0, std::move(invalidateBuffer));
  }

 private:
  static constexpr size_t kBlockSizeDefault = 4096;
  Device& dev_;
  size_t metadataSize_;
  const size_t blockSize_;
  uint64_t offset_{0};
  uint32_t bufIndex_{0};
  Buffer buffer_{blockSize_, blockSize_};
};

class DeviceMetaDataReader final : public RecordReader {
 public:
  explicit DeviceMetaDataReader(Device& dev, size_t metadataSize)
      : dev_{dev},
        metadataSize_{metadataSize},
        blockSize_{dev_.getIOAlignmentSize() >= kBlockSizeDefault
                       ? dev_.getIOAlignmentSize()
                       : kBlockSizeDefault} {}
  ~DeviceMetaDataReader() override = default;

  std::unique_ptr<folly::IOBuf> readRecord() override {
    bool readHeader = true;
    std::unique_ptr<folly::IOBuf> buf = nullptr;
    uint8_t* bufferData = buffer_.data();
    uint64_t size = 0;
    uint8_t* data = nullptr;
    auto dataOffset = 0;

    do {
      // This is true when we have to read a header and there are not
      // enough bytes in the buffer OR we have to read the next block
      // in the multi-block read
      if (bufIndex_ + headerSize() > blockSize_) {
        // read new block from the device if the number of bytes left from
        // previous read are less than header size.
        if (offset_ + blockSize_ > metadataSize_) {
          throw std::logic_error("exceeding metadata limit");
        }
        // read from device to the middle of the buffer 'kReadOffset'
        if (!dev_.read(offset_, blockSize_, bufferData)) {
          throw std::invalid_argument(
              folly::sformat("read failed: offset = {}", offset_));
        }
        offset_ += blockSize_;
        bufIndex_ = 0;
      }

      // Parse the header if we are expecting header
      if (readHeader) {
        readHeader = false;
        auto valid = validateRecordHeader(
            folly::Range<unsigned char*>(&bufferData[bufIndex_],
                                         blockSize_ - bufIndex_),
            kMetadataHeaderFileId);
        if (!valid) {
          throw std::logic_error("Invalid record header");
        }

        recordio_detail::Header* h =
            reinterpret_cast<recordio_detail::Header*>(&bufferData[bufIndex_]);
        size = headerSize() + h->dataLength;
        // copy the header also to IOBuf so that we can do validation
        buf = folly::IOBuf::create(size);
        if (buf == nullptr) {
          return nullptr;
        }
        buf->append(size);
        data = buf->writableData();
        dataOffset = 0;
      }
      auto cpSize =
          std::min(static_cast<uint64_t>(blockSize_ - bufIndex_), size);
      memcpy(data + dataOffset, &bufferData[bufIndex_], cpSize);
      bufIndex_ += cpSize;
      dataOffset += cpSize;
      size -= cpSize;
    } while (size > 0);
    // Validate the what we just read from the device
    auto record =
        validateRecordData(folly::Range<unsigned char*>(data, buf->length()));
    if (record.fileId == 0) {
      throw std::invalid_argument(folly::sformat(
          "Invalid record : offset = {}, length = {}", offset_, buf->length()));
    }
    // skip the header part and return
    buf->trimStart(headerSize());

    return buf;
  }

  bool isEnd() const override {
    Buffer headerBuf{blockSize_, blockSize_};
    if (offset_ + blockSize_ > metadataSize_) {
      return true;
    }
    auto res = dev_.read(offset_, blockSize_, headerBuf.data());
    if (!res) {
      return true;
    }
    auto valid = validateRecordHeader(
        folly::Range<unsigned char*>(headerBuf.data(), blockSize_),
        kMetadataHeaderFileId);

    return !valid;
  }

 private:
  static constexpr size_t kBlockSizeDefault = 4096;
  Device& dev_;
  size_t metadataSize_;
  const size_t blockSize_;
  uint64_t offset_{0};
  uint64_t bufIndex_{blockSize_};
  Buffer buffer_{blockSize_, blockSize_};
};

} // namespace

std::unique_ptr<RecordWriter> createMetadataRecordWriter(Device& dev,
                                                         size_t metadataSize) {
  return std::make_unique<DeviceMetaDataWriter>(dev, metadataSize);
}

std::unique_ptr<RecordReader> createMetadataRecordReader(Device& dev,
                                                         size_t metadataSize) {
  return std::make_unique<DeviceMetaDataReader>(dev, metadataSize);
}

std::unique_ptr<RecordWriter> createFileRecordWriter(int fd) {
  return std::make_unique<FileRecordWriter>(fd);
}

std::unique_ptr<RecordReader> createFileRecordReader(int fd) {
  return std::make_unique<FileRecordReader>(fd);
}

std::unique_ptr<RecordWriter> createFileRecordWriter(folly::File file) {
  return std::make_unique<FileRecordWriter>(std::move(file));
}

std::unique_ptr<RecordReader> createFileRecordReader(folly::File file) {
  return std::make_unique<FileRecordReader>(std::move(file));
}

} // namespace navy
} // namespace cachelib
} // namespace facebook

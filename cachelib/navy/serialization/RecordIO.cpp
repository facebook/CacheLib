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

#include <fmt/core.h>
#include <folly/Range.h>
#include <folly/io/RecordIO.h>

#include "cachelib/navy/common/Utils.h"

using namespace folly::recordio_helpers;

namespace facebook::cachelib::navy {
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
  uint64_t getCurPos() const override { return writer_.filePos(); }

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
  DeviceMetaDataWriter(Device& dev, size_t metadataSize, size_t stagingSize)
      : dev_(dev),
        metadataSize_{metadataSize},
        blockSize_{
            std::max<size_t>(dev_.getIOAlignmentSize(), kBlockSizeDefault)},
        stagingSize_{blockSize_ *
                     std::max<size_t>(1,
                                      (stagingSize == 0 ? kStagingSizeDefault
                                                        : stagingSize) /
                                          blockSize_)} {}

  ~DeviceMetaDataWriter() override {
    // The end-of-metadata marker below claims the region's last block, so a
    // tail that reaches the end of the region loses its final block.
    if (bufIndex_ > 0 && offset_ + stagedBlockBytes() >= metadataSize_) {
      bufIndex_ = stagedBlockBytes() - blockSize_;
    }
    if (bufIndex_ > 0) {
      writeStagedBlocks();
    }
    if (offset_ + blockSize_ <= metadataSize_) {
      // Write an additional block of zeroed out memory just to make the end
      // of metadata clear
      Buffer buffer = dev_.makeIOBuffer(blockSize_);
      memset(buffer.data(), 0, blockSize_);
      dev_.write(offset_, std::move(buffer));
    }

    XLOGF(INFO,
          "DeviceMetaDataWriter: wrote {} bytes out of total size: {}",
          offset_,
          metadataSize_);
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
    size_t dataOffset = 0;
    uint8_t* bufferData = buffer_.data();

    // The reader locates a header by scanning from a block boundary, so a
    // header may not straddle one.
    const size_t posInBlock = bufIndex_ % blockSize_;
    if (posInBlock + headerSize() > blockSize_) {
      const size_t skip = blockSize_ - posInBlock;
      memset(&bufferData[bufIndex_], 0, skip);
      bufIndex_ += skip;
    }
    if (offset_ + bufIndex_ + size > metadataSize_) {
      if (bufIndex_ != 0) {
        flushBuffer();
      }
      throw std::logic_error("exceeding metadata limit");
    }

    while (size > 0) {
      if (bufIndex_ == stagingSize_) {
        flushBuffer();
      }
      const auto cpBytes = std::min<uint64_t>(stagingSize_ - bufIndex_, size);
      memcpy(&bufferData[bufIndex_], data + dataOffset, cpBytes);
      dataOffset += cpBytes;
      bufIndex_ += cpBytes;
      size -= cpBytes;
    }
  }

  bool invalidate() override {
    Buffer invalidateBuffer{blockSize_, blockSize_};
    memset(invalidateBuffer.data(), 0, blockSize_);
    return dev_.write(0, std::move(invalidateBuffer));
  }

  uint64_t getCurPos() const override {
    return offset_ + bufIndex_ / blockSize_ * blockSize_;
  }

 private:
  static constexpr size_t kBlockSizeDefault = 4096;
  static constexpr size_t kStagingSizeDefault = 1024 * 1024;

  size_t stagedBlockBytes() const { return powTwoAlign(bufIndex_, blockSize_); }

  bool writeStagedBlocks() {
    const size_t flushSize = stagedBlockBytes();
    memset(buffer_.data() + bufIndex_, 0, flushSize - bufIndex_);
    if (!dev_.write(offset_, buffer_.view().slice(0, flushSize))) {
      return false;
    }
    offset_ += flushSize;
    bufIndex_ = 0;
    return true;
  }

  void flushBuffer() {
    if (!writeStagedBlocks()) {
      throw std::invalid_argument(
          fmt::format("write failed: offset = {}", offset_));
    }
  }

  Device& dev_;
  size_t metadataSize_;
  const size_t blockSize_;
  const size_t stagingSize_;
  uint64_t offset_{0};
  size_t bufIndex_{0};
  Buffer buffer_{stagingSize_, blockSize_};
};

class DeviceMetaDataReader final : public RecordReader {
 public:
  explicit DeviceMetaDataReader(Device& dev, size_t metadataSize)
      : dev_{dev},
        metadataSize_{metadataSize},
        blockSize_{
            std::max<size_t>(dev_.getIOAlignmentSize(), kBlockSizeDefault)} {}
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
              fmt::format("read failed: offset = {}", offset_));
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
      throw std::invalid_argument(fmt::format(
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

  // Block-granular: offset_ advances a block at a time, so this rounds up to
  // the block holding the last record read.
  uint64_t getCurPos() const override { return offset_; }

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
                                                         size_t metadataSize,
                                                         size_t stagingSize) {
  return std::make_unique<DeviceMetaDataWriter>(dev, metadataSize, stagingSize);
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

} // namespace facebook::cachelib::navy

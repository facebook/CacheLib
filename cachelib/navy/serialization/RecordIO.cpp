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

#include <limits>

#include "cachelib/navy/common/Utils.h"

using namespace folly::recordio_helpers;

namespace facebook::cachelib::navy {
constexpr uint32_t kMetadataHeaderFileId = 1;
namespace {
constexpr size_t kBlockSizeDefault = 4096;

size_t stagingBytes(size_t stagingSize, size_t blockSize) {
  return blockSize * std::max<size_t>(1, stagingSize / blockSize);
}

// Device::read takes a uint32_t length, so a staged read cannot exceed it.
size_t readStagingBytes(size_t stagingSize, size_t blockSize) {
  const size_t cap =
      blockSize * (std::numeric_limits<uint32_t>::max() / blockSize);
  return std::min(stagingBytes(stagingSize, blockSize), cap);
}

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
        stagingSize_{stagingBytes(stagingSize, blockSize_)} {}

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
  DeviceMetaDataReader(Device& dev, size_t metadataSize, size_t stagingSize)
      : dev_{dev},
        metadataSize_{metadataSize},
        blockSize_{
            std::max<size_t>(dev_.getIOAlignmentSize(), kBlockSizeDefault)},
        stagingSize_{readStagingBytes(stagingSize, blockSize_)} {}
  ~DeviceMetaDataReader() override = default;

  std::unique_ptr<folly::IOBuf> readRecord() override {
    skipBlockTailWithoutRoomForHeader();
    if (bufIndex_ == staged_) {
      stageNextChunk(blockSize_);
    }

    const uint8_t* bufferData = buffer_.data();
    if (!validateRecordHeader(
            folly::ByteRange{&bufferData[bufIndex_], staged_ - bufIndex_},
            kMetadataHeaderFileId)) {
      throw std::logic_error("Invalid record header");
    }
    const auto* h = reinterpret_cast<const recordio_detail::Header*>(
        &bufferData[bufIndex_]);
    // The header is copied into the IOBuf too so that the record can be
    // validated as a whole below.
    uint64_t size = headerSize() + h->dataLength;
    auto buf = folly::IOBuf::create(size);
    if (buf == nullptr) {
      return nullptr;
    }
    buf->append(size);
    uint8_t* data = buf->writableData();

    size_t dataOffset = 0;
    while (size > 0) {
      if (bufIndex_ == staged_) {
        stageNextChunk(size);
      }
      const auto cpSize = std::min<uint64_t>(staged_ - bufIndex_, size);
      memcpy(data + dataOffset, buffer_.data() + bufIndex_, cpSize);
      bufIndex_ += cpSize;
      dataOffset += cpSize;
      size -= cpSize;
    }

    auto record = validateRecordData(folly::ByteRange{data, buf->length()});
    if (record.fileId == 0) {
      throw std::invalid_argument(
          fmt::format("Invalid record : offset = {}, "
                      "length = {}",
                      getCurPos(),
                      buf->length()));
    }
    buf->trimStart(headerSize());

    return buf;
  }

  bool isEnd() const override {
    const size_t idx = nextHeaderIndex();
    if (idx < staged_) {
      return !validateRecordHeader(
          folly::ByteRange{buffer_.data() + idx, staged_ - idx},
          kMetadataHeaderFileId);
    }
    const uint64_t pos = stageBase_ + idx;
    if (pos + blockSize_ > metadataSize_) {
      return true;
    }
    Buffer headerBuf{blockSize_, blockSize_};
    if (!dev_.read(pos, static_cast<uint32_t>(blockSize_), headerBuf.data())) {
      return true;
    }
    return !validateRecordHeader(folly::ByteRange{headerBuf.data(), blockSize_},
                                 kMetadataHeaderFileId);
  }

  // Block-granular, so this rounds up to the block holding the last record
  // read rather than reporting the whole staged chunk as consumed.
  uint64_t getCurPos() const override {
    return stageBase_ + powTwoAlign(bufIndex_, blockSize_);
  }

 private:
  // The writer never lets a header straddle a block, so a block tail too
  // short to hold one is padding.
  size_t nextHeaderIndex() const {
    const size_t posInBlock = bufIndex_ % blockSize_;
    return posInBlock + headerSize() > blockSize_
               ? bufIndex_ + blockSize_ - posInBlock
               : bufIndex_;
  }

  void skipBlockTailWithoutRoomForHeader() { bufIndex_ = nextHeaderIndex(); }

  // Staging never runs past the blocks holding @needed, because the region
  // beyond the last record the writer flushed may be unwritten: on a sparse
  // or short-of-full device that reads back as a truncated IO, not zeroes.
  void stageNextChunk(size_t needed) {
    const uint64_t pos = stageBase_ + staged_;
    const size_t regionLeft = pos < metadataSize_ ? metadataSize_ - pos : 0;
    const size_t wanted =
        std::min(stagingSize_, powTwoAlign(needed, blockSize_));
    const size_t readSize =
        std::min(wanted, regionLeft) / blockSize_ * blockSize_;
    if (readSize == 0) {
      throw std::logic_error("exceeding metadata limit");
    }
    if (!dev_.read(pos, static_cast<uint32_t>(readSize), buffer_.data())) {
      throw std::invalid_argument(fmt::format("read failed: offset = {}", pos));
    }
    stageBase_ = pos;
    staged_ = readSize;
    bufIndex_ = 0;
  }

  Device& dev_;
  size_t metadataSize_;
  const size_t blockSize_;
  const size_t stagingSize_;
  uint64_t stageBase_{0};
  size_t staged_{0};
  size_t bufIndex_{0};
  Buffer buffer_{stagingSize_, blockSize_};
};

} // namespace

std::unique_ptr<RecordWriter> createMetadataRecordWriter(Device& dev,
                                                         size_t metadataSize,
                                                         size_t stagingSize) {
  return std::make_unique<DeviceMetaDataWriter>(dev, metadataSize, stagingSize);
}

std::unique_ptr<RecordReader> createMetadataRecordReader(Device& dev,
                                                         size_t metadataSize,
                                                         size_t stagingSize) {
  return std::make_unique<DeviceMetaDataReader>(dev, metadataSize, stagingSize);
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

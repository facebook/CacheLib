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

#include "cachelib/persistence/PersistenceManager.h"

#include <folly/File.h>
#include <folly/FileUtil.h>
#include <folly/hash/Checksum.h>

#include <fstream>

#include "cachelib/allocator/CacheAllocatorConfig.h"
#include "cachelib/common/Utils.h"

namespace facebook::cachelib::persistence {

using CopyBufferOp = folly::IOBuf::CopyBufferOp;

const char PersistenceManager::DATA_BEGIN_CHAR = static_cast<char>(28);
const char PersistenceManager::DATA_MARK_CHAR = static_cast<char>(30);
const char PersistenceManager::DATA_END_CHAR = static_cast<char>(31);

/**
 * Cache data (shm and navy) will be split into multiple data blocks to limit
 * the memory usage while restore, especially navy data might be much larger
 * than free memory. Read/write buffering is another benefit, it depends on the
 * stream reader/writer implementation, users can do the async buffering.
 *
 * Each block contains data length, checksum, and data up to kDataBlockSize
 * length. When we persist shm data with exact kDataBlockSize length, we want to
 * avoid the copy through DataBlock.data, and pass it directly to the writer,
 * we define a DataBlockHeader that only container checksum and data length.
 *
 * All block length except the last one is kDataBlockSize.
 * The last block might have a smaller length, if the total data size
 * (cache_size/navy_file_size) can't be divided by kDataBlockSize evenly.
 */
struct FOLLY_PACK_ATTR DataBlockHeader {
  uint32_t checksum;
  uint32_t length; // less or equal than kDataBlockSize
  void setLengthAndComputeChecksum(size_t len, const uint8_t* data) {
    XDCHECK_LE(len, static_cast<size_t>(kDataBlockSize));
    length = len;
    checksum = folly::crc32(data, length);
  }
};

struct FOLLY_PACK_ATTR DataBlock {
  DataBlockHeader header;
  uint8_t data[kDataBlockSize];
  void setData(size_t len, const uint8_t* ptr) {
    XDCHECK_LE(len, static_cast<size_t>(kDataBlockSize));
    ::memcpy(data, ptr, len);
    header.setLengthAndComputeChecksum(len, data);
  }
  bool validate() const {
    return header.checksum == folly::crc32(data, header.length);
  }
};

void PersistenceManager::saveCache(PersistenceStreamWriter& writer) {
  util::Timer timer;
  timer.startOrResume();

  XLOGF(INFO, "Start saving cache: cacheName {}, cacheDir {}",
        *config_.cacheName(), cacheDir_);
  writer.write(DATA_BEGIN_CHAR);

  // save versions
  {
    auto buf = Serializer::serializeToIOBuf(versions_);
    auto header = makeHeader(PersistenceType::Versions, buf->length());

    // The persisted stream consists of headers that are thrift serialized and
    // data blocks that are custom serialized in binary format to stream in
    // chunks. While restoring/deserializing from the stream, we want to read
    // from the stream only the bytes around the serialization boundaries to
    // simplify implementation. Hence,  we persist the size of thrift header in
    // binary format first so that we can read only that much from the stream
    // before proceeding to read/copy custom serialized blobs. Only one length
    // is persisted if we use BinarySerializer (fix encoding) not
    // CompactSerializer(variant encoding), this will be used to deserialize all
    // headers in restoreCache().
    size_t headerLength = header.length();

    writer.write(
        folly::IOBuf(CopyBufferOp::COPY_BUFFER, &headerLength, sizeof(size_t)));
    writer.write(header);
    writer.write(*buf);
  }

  // save configs
  {
    writer.write(DATA_MARK_CHAR);
    auto buf = Serializer::serializeToIOBuf(config_);
    writer.write(makeHeader(PersistenceType::Configs, buf->length()));
    writer.write(*buf);
  }

  // save meta data file (cache_dir/NvmCacheState)
  saveFile(writer, PersistenceType::NvmCacheState,
           NvmCacheState::getNvmCacheStateFilePath(cacheDir_));

  // save shm_info
  auto shmInfo =
      saveShm(writer, PersistenceType::ShmInfo, detail::kShmInfoName);

  // save shm_hash_table
  auto shmHT =
      saveShm(writer, PersistenceType::ShmHT, detail::kShmHashTableName);

  // save shm_chained_alloc_hash_table
  auto shmChainedHT = saveShm(writer, PersistenceType::ShmChainedItemHT,
                              detail::kShmChainedItemHashTableName);

  // save /dev/shm/shm_cache to multiple data blocks
  auto shmCache =
      saveShm(writer, PersistenceType::ShmData, detail::kShmCacheName);

  // save navy data
  {
    writer.write(DATA_MARK_CHAR);

    int32_t numBlocks =
        util::getAlignedSize(navyFileSize_, kDataBlockSize) / kDataBlockSize;
    writer.write(makeHeader(PersistenceType::NavyPartition, navyFileSize_));
    for (const std::string& file : navyFiles_) {
      folly::File f(file);
      for (int32_t i = 0; i < numBlocks; ++i) {
        DataBlock db;
        // readFull function read up to kDataBlockSize bytes from file
        // and return the num of bytes read.
        auto res = folly::readFull(f.fd(), db.data, kDataBlockSize);
        CACHELIB_CHECK_THROWF(res != -1, "fail to write file {}, errno: {}",
                              file, errno);
        db.header.setLengthAndComputeChecksum(res, db.data);
        writer.write(
            folly::IOBuf(CopyBufferOp::COPY_BUFFER, &db, sizeof(DataBlock)));
      }
    }
  }

  writer.write(DATA_END_CHAR);
  writer.flush();

  timer.pause();
  XLOGF(INFO, "saveCache finish, spent {} seconds", timer.getDurationSec());
}

void PersistenceManager::restoreCache(PersistenceStreamReader& reader) {
  util::Timer timer;
  timer.startOrResume();

  XLOGF(INFO, "Start restoring cache: cacheName {}, cacheDir {}",
        *config_.cacheName(), cacheDir_);

  CACHELIB_CHECK_THROW(reader.read() == DATA_BEGIN_CHAR,
                       "invalid beginning character");

  auto headerLengthBuf = reader.read(sizeof(size_t));
  size_t headerLength = cast<size_t>(headerLengthBuf.data());

  ShmManager::cleanup(cacheDir_, true);
  ShmManager shmManager(cacheDir_, true);
  SCOPE_SUCCESS { shmManager.shutDown(); };

  while (true) {
    auto headerBuf = reader.read(headerLength);
    CACHELIB_CHECK_THROW(headerBuf.length() == headerLength, "invalid data");
    auto header = deserialize<PersistenceHeader>(headerBuf);
    size_t dataLen = static_cast<size_t>(*header.length());

    XLOGF(INFO, "restoreCache: type {}, len {}, header_len {}",
          static_cast<int>(*header.type()), dataLen, headerLength);

    switch (*header.type()) {
    case PersistenceType::Versions: {
      auto buf = reader.read(dataLen);
      CACHELIB_CHECK_THROW(buf.length() == dataLen, "invalid data");
      deserializeAndValidateVersions(buf);
      break;
    }
    case PersistenceType::Configs: {
      auto buf = reader.read(dataLen);
      CACHELIB_CHECK_THROW(buf.length() == dataLen, "invalid data");

      auto config = deserialize<PersistCacheLibConfig>(buf);
      CACHELIB_CHECK_THROWF(config == config_, "Config doesn't match: {}|{}",
                            *config.cacheName(), *config_.cacheName());
      break;
    }
    case PersistenceType::NvmCacheState: {
      auto buf = reader.read(dataLen);
      CACHELIB_CHECK_THROW(buf.length() == dataLen, "invalid data");
      restoreFile(buf, NvmCacheState::getNvmCacheStateFilePath(cacheDir_));
      break;
    }
    case PersistenceType::ShmInfo: {
      auto shm = shmManager.createShm(detail::kShmInfoName, dataLen);
      restoreDataFromBlocks(reader, static_cast<uint8_t*>(shm.addr), dataLen);
      break;
    }
    case PersistenceType::ShmHT: {
      auto shm = shmManager.createShm(detail::kShmHashTableName, dataLen);
      restoreDataFromBlocks(reader, static_cast<uint8_t*>(shm.addr), dataLen);
      break;
    }
    case PersistenceType::ShmChainedItemHT: {
      auto shm =
          shmManager.createShm(detail::kShmChainedItemHashTableName, dataLen);
      restoreDataFromBlocks(reader, static_cast<uint8_t*>(shm.addr), dataLen);
      break;
    }
    case PersistenceType::ShmData: {
      ShmSegmentOpts opts;
      opts.alignment = sizeof(Slab); // 4MB
      auto shm = shmManager.createShm(detail::kShmCacheName, *header.length(),
                                      nullptr, opts);
      restoreDataFromBlocks(reader, static_cast<uint8_t*>(shm.addr),
                            *header.length());
      break;
    }
    case PersistenceType::NavyPartition: {
      int32_t navyFileSize = *header.length();
      int32_t numBlock =
          util::getAlignedSize(navyFileSize, kDataBlockSize) / kDataBlockSize;

      for (size_t i = 0; i < navyFiles_.size(); ++i) {
        folly::File f(navyFiles_[i], O_CREAT | O_WRONLY | O_TRUNC);
        for (int32_t j = 0; j < numBlock; ++j) {
          auto buf = reader.read(sizeof(DataBlock));
          CACHELIB_CHECK_THROW(buf.length() == sizeof(DataBlock),
                               "invalid data");
          const DataBlock& db = cast<DataBlock>(buf.data());
          CACHELIB_CHECK_THROW(db.validate(), "invalid checksum");
          auto res = folly::writeFull(f.fd(), db.data, db.header.length);
          CACHELIB_CHECK_THROWF(res != -1, "fail to write file {}, errno: {}",
                                navyFiles_[i], errno);
        }
      }
      break;
    }
    default:
      CACHELIB_CHECK_THROWF(false, "Unknow header type: {}",
                            static_cast<int>(*header.type()));
    }

    char mark = reader.read();
    switch (mark) {
    case DATA_MARK_CHAR:
      continue;
    case DATA_END_CHAR:
      timer.pause();
      XLOGF(INFO, "restoreCache finish, spent {} seconds",
            timer.getDurationSec());
      return;
    default:
      CACHELIB_CHECK_THROWF(false, "Unknown character: {}", mark);
    }
  }
}

folly::IOBuf PersistenceManager::makeHeader(PersistenceType type,
                                            size_t length) {
  PersistenceHeader header;
  header.type() = type;
  header.length() = length;
  // we must use apache::thrift::BinarySerializer not compact serializer,
  // so the integer is not compress (variant encoding)
  auto buf =
      Serializer::serializeToIOBuf<PersistenceHeader,
                                   apache::thrift::BinarySerializer>(header);
  return std::move(*buf);
}

void PersistenceManager::saveFile(PersistenceStreamWriter& writer,
                                  PersistenceType type,
                                  const folly::StringPiece file) {
  std::ifstream f(file.data());

  CACHELIB_CHECK_THROWF(f, "fail to open file to read {}", file.data());

  std::string buf{std::istreambuf_iterator<char>(f),
                  std::istreambuf_iterator<char>()};

  writer.write(DATA_MARK_CHAR);
  writer.write(makeHeader(type, buf.length()));
  writer.write(
      folly::IOBuf(CopyBufferOp::COPY_BUFFER, buf.data(), buf.length()));
}

void PersistenceManager::restoreFile(const folly::IOBuf& buf,
                                     const folly::StringPiece file) {
  if (buf.empty()) {
    return;
  }

  std::ofstream f(file.data(), std::ios::trunc);

  CACHELIB_CHECK_THROWF(f, "fail to open file to write {}", file.data());

  f.write(reinterpret_cast<const char*>(buf.data()), buf.length());
  if (f.good() && f.flush() && f.good()) {
    return;
  }
  // write failed
  CACHELIB_CHECK_THROWF(f, "fail to write to file {}, iostate: {}", file.data(),
                        static_cast<int>(f.rdstate()));
}

std::unique_ptr<ShmSegment> PersistenceManager::saveShm(
    PersistenceStreamWriter& writer,
    PersistenceType type,
    const std::string& name) {
  auto segment = ShmManager::attachShmReadOnly(cacheDir_, name, true);
  auto shm = segment->getCurrentMapping();
  CACHELIB_CHECK_THROWF(shm.size > 0, "shm {} is empty.", name);

  writer.write(DATA_MARK_CHAR);
  writer.write(makeHeader(type, shm.size));
  saveDataInBlocks(writer, shm);
  return segment;
}

void PersistenceManager::saveDataInBlocks(PersistenceStreamWriter& writer,
                                          const ShmAddr& shm) {
  const uint8_t* ptr = static_cast<uint8_t*>(shm.addr);
  uint32_t numBlock =
      util::getAlignedSize(shm.size, kDataBlockSize) / kDataBlockSize;
  const uint8_t* endPtr = ptr + shm.size;
  for (uint32_t i = 0; i < numBlock; ++i) {
    if (ptr + kDataBlockSize > endPtr) {
      // last data block might have less data than kDataBlockSize
      DataBlock db;
      db.setData(endPtr - ptr, ptr);
      writer.write(
          folly::IOBuf(CopyBufferOp::COPY_BUFFER, &db, sizeof(DataBlock)));
      ptr += db.header.length;
    } else {
      // data blocks other than the last one have exactly kDataBlockSize
      // length of data, we can avoid the extra data copy.
      DataBlockHeader dbh;
      dbh.setLengthAndComputeChecksum(kDataBlockSize, ptr);
      auto buf = folly::IOBuf(CopyBufferOp::COPY_BUFFER, &dbh,
                              sizeof(DataBlockHeader));
      // chained header and data to make a single write and be consistent with
      // restore
      buf.appendToChain(folly::IOBuf::wrapBuffer(ptr, kDataBlockSize));
      // we will trigger flush before shm dropped, so wrapBuffer is safe
      writer.write(std::move(buf));
      ptr += kDataBlockSize;
    }
  }
}

void PersistenceManager::restoreDataFromBlocks(PersistenceStreamReader& reader,
                                               uint8_t* ptr,
                                               size_t size) {
  uint32_t numBlock =
      util::getAlignedSize(size, kDataBlockSize) / kDataBlockSize;
  for (uint32_t i = 0; i < numBlock; ++i) {
    auto buf = reader.read(sizeof(DataBlock));
    CACHELIB_CHECK_THROW(buf.length() == sizeof(DataBlock), "invalid data");
    const DataBlock& db = cast<DataBlock>(buf.data());
    CACHELIB_CHECK_THROW(db.validate(), "invalid checksum");
    ::memcpy(ptr, db.data, db.header.length);
    ptr += db.header.length;
  }
}

void PersistenceManager::deserializeAndValidateVersions(
    const folly::IOBuf& buf) {
  auto versions = deserialize<CacheLibVersions>(buf);
  CACHELIB_CHECK_THROWF(*versions.persistenceVersion() ==
                            *versions_.persistenceVersion(),
                        "Persistence Version doesn't match: {}|{}",
                        *versions.persistenceVersion(),
                        *versions_.persistenceVersion());
  if (versions != versions_) {
    // print a warning log for cache version mismatch,
    // attaching the cache will might be anble to do upgrade
    // even cache format changed.
    XLOGF(WARN, "Cache Version doesn't match: {}|{} {}|{}, {}|{}",
          *versions.allocatorVersion(), *versions_.allocatorVersion(),
          *versions.ramFormatVerson(), *versions_.ramFormatVerson(),
          *versions.nvmFormatVersion(), *versions_.nvmFormatVersion());
  }
}

} // namespace facebook::cachelib::persistence

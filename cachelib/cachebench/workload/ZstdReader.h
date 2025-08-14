#pragma once

#include <fstream>
#include <memory>
#include <vector>
#include <zstd.h>
#include <cstdint>
#include <cstring> // For std::memmove
#include <folly/logging/xlog.h> // Include folly logging

namespace facebook {
namespace cachelib {
namespace cachebench {

struct OracleGeneralBinRequest {
    uint32_t clockTime;
    uint64_t objId;
    uint32_t objSize;
    int64_t nextAccessVtime;
};

class ZstdReader {
public:
    ZstdReader(bool compressed = true);
    ~ZstdReader();

    ZstdReader(ZstdReader&& other) noexcept;
    ZstdReader& operator=(ZstdReader&& other) noexcept;

    void open(const std::string& trace_path, bool compressed = true);
    size_t read_bytes(size_t n_byte, char** data_start);
    bool read_one_req(OracleGeneralBinRequest* req);
    void close();
    bool is_open() const;

private:
    std::ifstream ifile;
    bool compressed_{true}; // <--- NEW FIELD

    // Only used if compressed_
    std::unique_ptr<ZSTD_DStream, decltype(&ZSTD_freeDStream)> zds;
    std::vector<char> buff_in;
    std::vector<char> buff_out;
    size_t buff_out_read_pos;
    ZSTD_inBuffer input;
    ZSTD_outBuffer output;

    enum class Status { OK, ERR, MY_EOF } status;

    size_t read_from_file();
    Status decompress_from_buff();
};

// --- Implementation ---

ZstdReader::ZstdReader(bool compressed)
    : compressed_(compressed),
      zds(compressed ? ZSTD_createDStream() : nullptr, ZSTD_freeDStream),
      buff_in(compressed ? ZSTD_DStreamInSize() : 0),
      buff_out(compressed ? ZSTD_DStreamOutSize() * 2 : 0),
      buff_out_read_pos(0),
      status(Status::OK) {
    if (compressed_) {
        input.src = buff_in.data();
        input.size = 0;
        input.pos = 0;
        output.dst = buff_out.data();
        output.size = buff_out.size();
        output.pos = 0;
        ZSTD_initDStream(zds.get());
    }
}

ZstdReader::~ZstdReader() {
    close();
}

ZstdReader::ZstdReader(ZstdReader&& other) noexcept
    : ifile(std::move(other.ifile)),
      compressed_(other.compressed_),
      zds(std::move(other.zds)),
      buff_in(std::move(other.buff_in)),
      buff_out(std::move(other.buff_out)),
      buff_out_read_pos(other.buff_out_read_pos),
      input(other.input),
      output(other.output),
      status(other.status) {
    other.buff_out_read_pos = 0;
    other.input = {nullptr, 0, 0};
    other.output = {nullptr, 0, 0};
    other.status = Status::ERR;
}

ZstdReader& ZstdReader::operator=(ZstdReader&& other) noexcept {
    if (this != &other) {
        close();
        ifile = std::move(other.ifile);
        compressed_ = other.compressed_;
        zds = std::move(other.zds);
        buff_in = std::move(other.buff_in);
        buff_out = std::move(other.buff_out);
        buff_out_read_pos = other.buff_out_read_pos;
        input = other.input;
        output = other.output;
        status = other.status;
        other.buff_out_read_pos = 0;
        other.input = {nullptr, 0, 0};
        other.output = {nullptr, 0, 0};
        other.status = Status::ERR;
    }
    return *this;
}

void ZstdReader::open(const std::string& trace_path, bool compressed) {
    compressed_ = compressed;
    ifile.open(trace_path, std::ios::binary);
    if (!ifile.is_open()) {
        throw std::runtime_error("Cannot open file: " + trace_path);
    }
    if (compressed_) {
        if (!zds) {
            zds.reset(ZSTD_createDStream());
        }
        if (buff_in.empty()) buff_in.resize(ZSTD_DStreamInSize());
        if (buff_out.empty()) buff_out.resize(ZSTD_DStreamOutSize() * 2);
        buff_out_read_pos = 0;
        input.src = buff_in.data();
        input.size = 0;
        input.pos = 0;
        output.dst = buff_out.data();
        output.size = buff_out.size();
        output.pos = 0;
        ZSTD_initDStream(zds.get());
    }
}

void ZstdReader::close() {
    if (ifile.is_open()) {
        ifile.close();
    }
}

bool ZstdReader::is_open() const {
    return ifile.is_open();
}

size_t ZstdReader::read_from_file() {
    ifile.read(buff_in.data(), buff_in.size());
    size_t read_sz = ifile.gcount();
    if (read_sz < buff_in.size()) {
        if (ifile.eof()) {
            status = Status::MY_EOF;
        } else {
            status = Status::ERR;
            return 0;
        }
    }
    input.size = read_sz;
    input.pos = 0;
    return read_sz;
}

ZstdReader::Status ZstdReader::decompress_from_buff() {
    std::memmove(buff_out.data(), buff_out.data() + buff_out_read_pos, output.pos - buff_out_read_pos);
    output.pos -= buff_out_read_pos;
    buff_out_read_pos = 0;

    if (input.pos >= input.size) {
        size_t read_sz = read_from_file();
        if (read_sz == 0) {
            if (status == Status::MY_EOF) {
                return Status::MY_EOF;
            } else {
                XLOG(ERR) << "Read from file error";
                return Status::ERR;
            }
        }
    }

    size_t const ret = ZSTD_decompressStream(zds.get(), &output, &input);
    if (ret != 0 && ZSTD_isError(ret)) {
        XLOG(ERR) << "Zstd decompression error: " << ZSTD_getErrorName(ret);
    }

    return Status::OK;
}

size_t ZstdReader::read_bytes(size_t n_byte, char** data_start) {
    if (!compressed_) {
        // Uncompressed: read directly from file
        static std::vector<char> plain_buff;
        if (plain_buff.size() < n_byte) plain_buff.resize(n_byte);
        ifile.read(plain_buff.data(), n_byte);
        size_t bytes_read = ifile.gcount();
        if (bytes_read != n_byte) {
            return 0;
        }
        *data_start = plain_buff.data();
        return bytes_read;
    }

    // Compressed: use existing logic
    size_t sz = 0;
    while (buff_out_read_pos + n_byte > output.pos) {
        Status status = decompress_from_buff();
        if (status != Status::OK) {
            if (status != Status::MY_EOF) {
                XLOG(ERR) << "Error decompressing file";
            } else {
                return 0;
            }
            break;
        }
    }
    if (buff_out_read_pos + n_byte <= output.pos) {
        sz = n_byte;
        *data_start = buff_out.data() + buff_out_read_pos;
        buff_out_read_pos += n_byte;
    } else {
        XLOG(ERR) << "Do not have enough bytes " << output.pos - buff_out_read_pos << " < " << n_byte;
    }
    return sz;
}

bool ZstdReader::read_one_req(OracleGeneralBinRequest* req) {
    char* record;
    size_t bytes_read = read_bytes(24, &record);
    if (bytes_read != 24) {
        return false;
    }
    req->clockTime = *(uint32_t*)record;
    req->objId = *(uint64_t*)(record + 4);
    req->objSize = *(uint32_t*)(record + 12);
    req->nextAccessVtime = *(int64_t*)(record + 16);
    if (req->nextAccessVtime == -1 || req->nextAccessVtime == INT64_MAX) {
        req->nextAccessVtime = INT64_MAX;
    }
    if (req->objSize == 0) {
        return read_one_req(req);
    }
    return true;
}

} // namespace cachebench
} // namespace cachelib
} // namespace facebook
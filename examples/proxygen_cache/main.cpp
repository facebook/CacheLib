#include <proxygen/httpserver/HTTPServer.h>
#include <proxygen/httpserver/RequestHandler.h>
#include <proxygen/httpserver/RequestHandlerFactory.h>
#include <proxygen/httpserver/ResponseBuilder.h>
#include <proxygen/httpserver/Filters.h>
#include <proxygen/httpserver/HTTPServerOptions.h>
#include <wangle/ssl/SSLContextConfig.h>

#include <folly/init/Init.h>
#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/Optional.h>

#include <glog/logging.h>

#include <cachelib/allocator/CacheAllocator.h>
#include <cachelib/allocator/CacheAllocatorConfig.h>
#include <cachelib/allocator/PoolOptimizeStrategy.h>

#include <atomic>
#include <memory>
#include <string>
#include <utility>

using proxygen::HTTPMessage;
using proxygen::HTTPServer;
using proxygen::RequestHandler;
using proxygen::RequestHandlerFactory;
using proxygen::ResponseBuilder;

namespace {

using Cache = facebook::cachelib::LruAllocator;
using CacheConfig = facebook::cachelib::LruAllocator::Config;
using WriteHandle = Cache::WriteHandle;
using ReadHandle = Cache::ReadHandle;

struct CacheCtx {
  std::unique_ptr<Cache> cache;
  facebook::cachelib::PoolId poolId{};
};

class CacheHandler : public RequestHandler {
 public:
  explicit CacheHandler(CacheCtx* ctx) : ctx_(ctx) {}

  void onRequest(std::unique_ptr<HTTPMessage> headers) noexcept override {
    req_ = std::move(headers);
  }

  void onBody(std::unique_ptr<folly::IOBuf> body) noexcept override {
    if (body) {
      bodyQueue_.append(std::move(body));
    }
  }

  void onEOM() noexcept override {
    try {
      if (!req_) {
        sendError(400, "Bad Request");
        return;
      }
      const auto method = req_->getMethod();
      const auto path = req_->getPath(); // e.g. /cache/foo
      std::string key;
      if (!extractKey(path, key)) {
        sendError(404, "Not Found");
        return;
      }

      if (method == proxygen::HTTPMethod::GET) {
        handleGet(key);
      } else if (method == proxygen::HTTPMethod::PUT) {
        handlePut(key);
      } else if (method == proxygen::HTTPMethod::DELETE) {
        handleDelete(key);
      } else {
        ResponseBuilder(downstream_)
          .status(405, "Method Not Allowed")
          .header("Allow", "GET, PUT, DELETE")
          .body("Method Not Allowed\n")
          .sendWithEOM();
      }
    } catch (const std::exception& ex) {
      LOG(ERROR) << "Request handling exception: " << ex.what();
      sendError(500, "Internal Server Error");
    }
  }

  void onUpgrade(proxygen::UpgradeProtocol /*protocol*/) noexcept override {}
  void requestComplete() noexcept override { delete this; }
  void onError(proxygen::ProxygenError /*err*/) noexcept override { delete this; }

 private:
  bool extractKey(const std::string& path, std::string& keyOut) {
    // Expect paths of form /cache/<key>
    // Allow any characters after /cache/; you may want to URL-decode in real apps.
    static constexpr char kPrefix[] = "/cache/";
    if (path.rfind(kPrefix, 0) != 0) {
      return false;
    }
    keyOut = path.substr(sizeof(kPrefix) - 1);
    return !keyOut.empty();
  }

  void handleGet(const std::string& key) {
    ReadHandle h = ctx_->cache->find(key);
    if (!h) {
      ResponseBuilder(downstream_)
        .status(404, "Not Found")
        .body("Key not found\n")
        .sendWithEOM();
      return;
    }
    auto data = reinterpret_cast<const char*>(h->getMemory());
    auto size = h->getSize();
    ResponseBuilder(downstream_)
      .status(200, "OK")
      .header("Content-Type", "application/octet-stream")
      .body(folly::StringPiece(data, size))
      .sendWithEOM();
  }

  void handlePut(const std::string& key) {
    // Flatten body
    std::unique_ptr<folly::IOBuf> buf = bodyQueue_.move();
    auto body = buf->coalesce();
    if (body.size() == 0) {
      ResponseBuilder(downstream_)
        .status(400, "Bad Request")
        .body("Empty body\n")
        .sendWithEOM();
      return;
    }

    WriteHandle wh = ctx_->cache->allocate(ctx_->poolId, key, body.size());
    if (!wh) {
      // Allocation failed (e.g., item too big); try eviction via remove + retry could be added.
      ResponseBuilder(downstream_)
        .status(507, "Insufficient Storage")
        .body("Allocation failed\n")
        .sendWithEOM();
      return;
    }

    std::memcpy(wh->getMemory(), body.data(), body.size());
    ctx_->cache->insertOrReplace(std::move(wh));

    ResponseBuilder(downstream_)
      .status(201, "Created")
      .body("OK\n")
      .sendWithEOM();
  }

  void handleDelete(const std::string& key) {
    auto res = ctx_->cache->remove(key);
    bool removed = true;
    ResponseBuilder(downstream_)
      .status(removed ? 204 : 404, removed ? "No Content" : "Not Found")
      .sendWithEOM();
  }

  void sendError(uint16_t code, folly::StringPiece msg) {
    ResponseBuilder(downstream_)
      .status(code, std::string(msg))
      .body(folly::to<std::string>(msg, "\n"))
      .sendWithEOM();
  }

  CacheCtx* ctx_;
  std::unique_ptr<HTTPMessage> req_;
  folly::IOBufQueue bodyQueue_{folly::IOBufQueue::cacheChainLength()};
};

class CacheHandlerFactory : public RequestHandlerFactory {
 public:
  explicit CacheHandlerFactory(std::shared_ptr<CacheCtx> ctx) : ctx_(std::move(ctx)) {}

  void onServerStart(folly::EventBase*) noexcept override {}
  void onServerStop() noexcept override {}

  RequestHandler* onRequest(RequestHandler*, HTTPMessage*) noexcept override {
    return new CacheHandler(ctx_.get());
  }

 private:
  std::shared_ptr<CacheCtx> ctx_;
};

std::unique_ptr<Cache> buildCache(size_t cacheBytes, facebook::cachelib::PoolId& poolOut) {
  CacheConfig config;
  config.setCacheName("proxygen-cache");
  config.setCacheSize(cacheBytes);

  auto cache = std::make_unique<Cache>(config);
  auto numBytes = cache->getCacheMemoryStats().ramCacheSize;

  LOG(INFO) << "creating cache with size " << numBytes << " bytes";
  // Make a single pool covering all memory
  poolOut = cache->addPool("default", numBytes);
  return cache;
}

} // anonymous namespace

int main(int argc, char* argv[]) {
  folly::Init init(&argc, &argv);
  FLAGS_logtostderr   = 1;  // show logs on stderr
  FLAGS_minloglevel   = 0;  // include INFO
  FLAGS_stderrthreshold = 0;
  FLAGS_v             = 2;  // enable VLOG(1..2)
  // Flags (basic): port and cache size
  uint16_t port = 8111;
  size_t cacheBytes = 1024*1024*1024; // 1 GB
  if (argc >= 2) {
    port = static_cast<uint16_t>(std::stoi(argv[1]));
  }
  if (argc >= 3) {
    cacheBytes = folly::to<size_t>(argv[2]); // bytes
  }

  auto ctx = std::make_shared<CacheCtx>();
  ctx->cache = buildCache(cacheBytes, ctx->poolId);

  proxygen::HTTPServerOptions options;
  options.threads = static_cast<size_t>(std::thread::hardware_concurrency());
  options.idleTimeout = std::chrono::milliseconds(60000);
  options.shutdownOn = {SIGINT, SIGTERM};
  options.handlerFactories = proxygen::RequestHandlerChain()
    .addThen<CacheHandlerFactory>(ctx)
    .build();

  HTTPServer server(std::move(options));
  std::vector<HTTPServer::IPConfig> ips = {
      {folly::SocketAddress("0.0.0.0", port, true), proxygen::HTTPServer::Protocol::HTTP}
  };
  server.bind(ips);

  LOG(INFO) << "Listening on port " << port;
  server.start();
  return 0;
}


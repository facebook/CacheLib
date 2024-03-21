# [Choice] Ubuntu version (use ubuntu-22.04 or ubuntu-18.04 on local arm64/Apple Silicon): ubuntu-22.04, ubuntu-20.04, ubuntu-18.04
ARG VARIANT=ubuntu-20.04
FROM mcr.microsoft.com/vscode/devcontainers/base:0-${VARIANT}

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends build-essential \
  git \
  g++ \
  cmake \
  bison flex \
  libboost-all-dev \
  libevent-dev \
  libdouble-conversion-dev \
  libgoogle-glog-dev \
  libgflags-dev \
  libiberty-dev \
  liblz4-dev \
  liblzma-dev \
  libbz2-dev \
  libsnappy-dev \
  make \
  zlib1g-dev \
  binutils-dev \
  libjemalloc-dev \
  libssl-dev \
  pkg-config \
  libunwind-dev \
  libelf-dev \
  libdwarf-dev \
  libsodium-dev \
  libaio-dev \
  libnuma-dev

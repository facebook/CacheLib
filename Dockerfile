# Dockerfile — Option A with DTO-provided cachelib.patch applied to CacheLib
FROM ubuntu:24.04
SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ARG DEBIAN_FRONTEND=noninteractive
ARG DTO_REPO=https://github.com/intel/DTO.git
ARG DTO_BRANCH=cachelib
# If your patch path inside DTO is different, override at build:
#   --build-arg DTO_PATCH_PATH=patches/cachelib.patch
ARG DTO_PATCH_PATH=
# Accept patch via CLI
ARG GETDEPS_PATCH_B64=""
ARG GETDEPS_PATCH_URL=""
ARG JOBS=16

RUN apt-get update && apt-get install -y software-properties-common \
    && add-apt-repository -y universe && apt-get update
RUN apt-get install -y libboost-all-dev

# 1) Base toolchain + libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git python3 build-essential cmake ninja-build pkg-config \
    autoconf automake libtool patch \
    accel-config uuid-dev linux-libc-dev libaccel-config-dev \
    sudo python3-pip \
    libssl-dev zlib1g-dev libunwind-dev libevent-dev libnuma-dev libaio-dev \
    vim less libbz2-dev

# Make sure /usr/local is searched for headers/libs and pkg-config
ENV LD_LIBRARY_PATH=/usr/local/lib
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig
ENV CMAKE_PREFIX_PATH=/usr/local

# 2) Bring in your CacheLib sources
WORKDIR /src/CacheLib
COPY . .

# 3) Clone DTO (cachelib branch), locate the patch, apply to CacheLib
WORKDIR /opt/dto
RUN git clone --depth=1 --branch "${DTO_BRANCH}" "${DTO_REPO}" . && \
    set -eux; \
    # Determine patch file path:
    if [ -n "${DTO_PATCH_PATH}" ] && [ -f "${DTO_PATCH_PATH}" ]; then \
      PATCH_FILE="${DTO_PATCH_PATH}"; \
    else \
      # Try common locations, otherwise auto-discover the first match
      if   [ -f /opt/dto/cachelib.patch ]; then PATCH_FILE=/opt/dto/cachelib.patch; \
      elif [ -f /opt/dto/patches/cachelib.patch ]; then PATCH_FILE=/opt/dto/patches/cachelib.patch; \
      else PATCH_FILE="$(find /opt/dto -maxdepth 4 -type f -iname '*cachelib*.patch' -print -quit)"; fi; \
    fi; \
    test -n "${PATCH_FILE}" && test -f "${PATCH_FILE}" || { echo 'cachelib patch not found in DTO repo'; exit 1; }; \
    echo "Using DTO patch: ${PATCH_FILE}"; \
    # Apply patch to CacheLib tree
    git -C /src/CacheLib apply "${PATCH_FILE}" || (cd /src/CacheLib && patch -p1 < "${PATCH_FILE}")

# 4) Install system deps via getdeps (after patch is applied)

# Apply getdeps patch if provided
WORKDIR /src/CacheLib
RUN set -euo pipefail \
 && if [[ -n "${GETDEPS_PATCH_B64:-}" ]]; then \
      echo "${GETDEPS_PATCH_B64}" | base64 -d > /tmp/getdeps.patch; \
    elif [[ -n "${GETDEPS_PATCH_URL:-}" ]]; then \
      apt-get update && apt-get install -y --no-install-recommends ca-certificates curl; \
      curl -fsSL "${GETDEPS_PATCH_URL}" -o /tmp/getdeps.patch; \
    fi \
 && if [[ -s /tmp/getdeps.patch ]]; then \
      echo "Checking getdeps patch applicability..."; \
      if patch -p1 -N --dry-run < /tmp/getdeps.patch >/dev/null 2>&1; then \
        echo "Applying getdeps patch"; \
        patch -p1 -N < /tmp/getdeps.patch; \
      else \
        echo "Skipping getdeps patch (already applied or not applicable)"; \
      fi; \
    else \
      echo "No getdeps patch provided; skipping."; \
    fi

WORKDIR /src/CacheLib
RUN python3 build/fbcode_builder/getdeps.py \
      --allow-system-packages install-system-deps --recursive cachelib

# 5) Build & install DTO (so CacheLib can link against it)
WORKDIR /opt/dto
RUN cmake -S . -B build \
      -DCMAKE_BUILD_TYPE=Release \
      -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
      -DCMAKE_INSTALL_PREFIX=/usr/local && \
    cmake --build build -j"${JOBS}" && \
    cmake --install build && \
    ldconfig

# 6) Build CacheLib (no tests), then fix up runtime deps into /usr/local
WORKDIR /src/CacheLib
RUN python3 build/fbcode_builder/getdeps.py \
      --allow-system-packages build --src-dir=. cachelib --project-install-prefix cachelib:/usr/local

RUN python3 build/fbcode_builder/getdeps.py \
      --allow-system-packages fixup-dyn-deps --strip --src-dir=. \
      cachelib /opt/cachelib-runtime \
      --project-install-prefix cachelib:/usr/local \
      --final-install-prefix /opt/cachelib-runtime

RUN if [ -d /opt/cachelib-runtime/usr/local ]; then \
      mv /opt/cachelib-runtime/usr/local/* /opt/cachelib-runtime/; \
      rmdir /opt/cachelib-runtime/usr/local || true; \
    fi
    
#RUN mkdir -p /opt/artifacts && \
#    python3 build/fbcode_builder/getdeps.py \
#      --allow-system-packages fixup-dyn-deps --strip --src-dir=. cachelib /opt/artifacts \
#      --project-install-prefix cachelib:/usr/local --final-install-prefix /usr/local && \
#    cp -a /opt/artifacts/usr/local/. /usr/local/ && \
#    ldconfig

# 7) Prove it runs (no tests). Override CMD with any cachebench args you want.
#ENTRYPOINT ["/usr/local/bin/cachebench"]
#CMD ["--help"]


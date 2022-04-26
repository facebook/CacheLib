FROM quay.io/centos/centos:stream8

RUN dnf install -y \
cmake \
sudo \
git \
tzdata \
vim \
gdb \
clang

COPY ./install-cachelib-deps.sh ./install-cachelib-deps.sh
RUN ./install-cachelib-deps.sh

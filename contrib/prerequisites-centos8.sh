#!/bin/sh

sudo dnf install -y epel-release
sudo dnf groupinstall -y "Development Tools"
sudo dnf install -y \
  libtool bison flex cmake \
  double-conversion double-conversion-devel \
  boost-devel \
  libevent-devel \
  openssl-devel \
  libunwind-devel \
  zlib-devel \
  lz4-devel \
  libzstd-devel \
  xz-devel \
  bzip2-devel \
  jemalloc-devel \
  libsodium-devel \
  libaio-devel \
  binutils-devel


# Install and enable the "PowerTools" repository
sudo dnf -y install dnf-plugins-core
sudo dnf -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-8.noarch.rpm

# CentOS Repository name changed from "PowerTools" to "powertools" (and back?)
# Get the list of ALL repositories, try to detect the 'powertools' one.
# We could do:
#    dnf config-manager --set-enabled PowerTools
#    dnf config-manager --set-enabled powertools
# But we don't want to change the system's repository configuration.

# Show list of repos (for debugging)
sudo dnf repolist --all

POWERTOOLS_REPO=$(sudo dnf repolist --all | grep -i '^powertools' | awk '{print $1}')
echo "Detected CentOS Powertools repository: '$POWERTOOLS_REPO'"

sudo dnf --enablerepo="$POWERTOOLS_REPO" install -y \
  libdwarf-devel \
  snappy-devel \
  gmock-devel \
  gtest-devel \
  libsodium-static \
  libdwarf-static \
  boost-static \
  double-conversion-static

#Do not install these from OS packages - they are typically outdated.
#gflags-devel \
#glog \
#fmt \

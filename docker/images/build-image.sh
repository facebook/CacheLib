#!/usr/bin/env bash
# SPDX-License-Identifier: BSD-3-Clause
# Copyright 2016-2021, Intel Corporation
#
# build-image.sh - prepares a Docker image with <OS>-based environment for
#		testing (or dev) purpose, tagged with ${CONTAINER_REG}:${OS}-${OS_VER}-${IMG_VER},
#		according to the ${OS}-${OS_VER}.Dockerfile file located in the same directory.
#		IMG_VER is a version of Docker image (it usually relates to project's release tag)
#		and it defaults to "devel".
#

set -e
IMG_VER=${IMG_VER:-devel}
TAG="${OS}-${OS_VER}-${IMG_VER}"

if [[ -z "${OS}" || -z "${OS_VER}" ]]; then
	echo "ERROR: The variables OS and OS_VER have to be set " \
		"(e.g. OS=fedora, OS_VER=34)."
	exit 1
fi

if [[ -z "${CONTAINER_REG}" ]]; then
	echo "ERROR: CONTAINER_REG environment variable is not set " \
		"(e.g. \"<registry_addr>/<org_name>/<package_name>\")."
	exit 1
fi

echo "Check if the file ${OS}-${OS_VER}.Dockerfile exists"
if [[ ! -f "${OS}-${OS_VER}.Dockerfile" ]]; then
	echo "Error: ${OS}-${OS_VER}.Dockerfile does not exist."
	exit 1
fi

echo "Build a Docker image tagged with: ${CONTAINER_REG}:${TAG}"
docker build -t ${CONTAINER_REG}:${TAG} \
	--build-arg http_proxy=$http_proxy \
	--build-arg https_proxy=$https_proxy \
	-f ${OS}-${OS_VER}.Dockerfile .

#!/usr/bin/env bash
# SPDX-License-Identifier: BSD-3-Clause
# Copyright 2022, Intel Corporation

#
# build.sh - runs a Docker container from a Docker image with environment
#		prepared for running CacheLib builds and tests. It uses Docker image
#		tagged as described in ./images/build-image.sh.
#
# Notes:
# - set env var 'HOST_WORKDIR' to where the root of this project is on the host machine,
# - set env var 'OS' and 'OS_VER' properly to a system/Docker you want to build this
#	repo on (for proper values take a look at the list of Dockerfiles at the
#	utils/docker/images directory in this repo), e.g. OS=ubuntu, OS_VER=20.04,
# - set env var 'CONTAINER_REG' to container registry address
#	[and possibly user/org name, and package name], e.g. "<CR_addr>/pmem/CacheLib",
# - set env var 'DNS_SERVER' if you use one,
# - set env var 'COMMAND' to execute specific command within Docker container or
#	env var 'TYPE' to pick command based on one of the predefined types of build (see below).
#

set -e

source $(dirname ${0})/set-ci-vars.sh
IMG_VER=${IMG_VER:-devel}
TAG="${OS}-${OS_VER}-${IMG_VER}"
IMAGE_NAME=${CONTAINER_REG}:${TAG}
CONTAINER_NAME=CacheLib-${OS}-${OS_VER}
WORKDIR=/CacheLib  # working dir within Docker container
SCRIPTSDIR=${WORKDIR}/docker

if [[ -z "${OS}" || -z "${OS_VER}" ]]; then
	echo "ERROR: The variables OS and OS_VER have to be set " \
		"(e.g. OS=fedora, OS_VER=32)."
	exit 1
fi

if [[ -z "${HOST_WORKDIR}" ]]; then
	echo "ERROR: The variable HOST_WORKDIR has to contain a path to " \
		"the root of this project on the host machine."
	exit 1
fi

if [[ -z "${CONTAINER_REG}" ]]; then
	echo "ERROR: CONTAINER_REG environment variable is not set " \
		"(e.g. \"<registry_addr>/<org_name>/<package_name>\")."
	exit 1
fi

# Set command to execute in the Docker container
COMMAND="./run-build.sh";
echo "COMMAND to execute within Docker container: ${COMMAND}"

if [ -n "${DNS_SERVER}" ]; then DOCKER_OPTS="${DOCKER_OPTS} --dns=${DNS_SERVER}"; fi

# Check if we are running on a CI (Travis or GitHub Actions)
[ -n "${GITHUB_ACTIONS}" -o -n "${TRAVIS}" ] && CI_RUN="YES" || CI_RUN="NO"

# Do not allocate a pseudo-TTY if we are running on GitHub Actions
[ ! "${GITHUB_ACTIONS}" ] && DOCKER_OPTS="${DOCKER_OPTS} --tty=true"


echo "Running build using Docker image: ${IMAGE_NAME}"

# Run a container with
#  - environment variables set (--env)
#  - host directory containing source mounted (-v)
#  - working directory set (-w)
docker run --privileged=true --name=${CONTAINER_NAME} -i \
	${DOCKER_OPTS} \
	--env http_proxy=${http_proxy} \
	--env https_proxy=${https_proxy} \
	--env TERM=xterm-256color \
	--env WORKDIR=${WORKDIR} \
	--env SCRIPTSDIR=${SCRIPTSDIR} \
	--env GITHUB_REPO=${GITHUB_REPO} \
	--env CI_RUN=${CI_RUN} \
	--env TRAVIS=${TRAVIS} \
	--env GITHUB_ACTIONS=${GITHUB_ACTIONS} \
	--env CI_COMMIT=${CI_COMMIT} \
	--env CI_COMMIT_RANGE=${CI_COMMIT_RANGE} \
	--env CI_BRANCH=${CI_BRANCH} \
	--env CI_EVENT_TYPE=${CI_EVENT_TYPE} \
	--env CI_REPO_SLUG=${CI_REPO_SLUG} \
	--env DOC_UPDATE_GITHUB_TOKEN=${DOC_UPDATE_GITHUB_TOKEN} \
	--env DOC_UPDATE_BOT_NAME=${DOC_UPDATE_BOT_NAME} \
	--env DOC_REPO_OWNER=${DOC_REPO_OWNER} \
	--env COVERITY_SCAN_TOKEN=${COVERITY_SCAN_TOKEN} \
	--env COVERITY_SCAN_NOTIFICATION_EMAIL=${COVERITY_SCAN_NOTIFICATION_EMAIL} \
	--env TEST_TIMEOUT=${TEST_TIMEOUT} \
	--env TZ='Europe/Warsaw' \
	--shm-size=4G \
	-v ${HOST_WORKDIR}:${WORKDIR} \
	-v /etc/localtime:/etc/localtime \
	-w ${SCRIPTSDIR} \
	${IMAGE_NAME} ${COMMAND}


#!/usr/bin/env bash
# SPDX-License-Identifier: BSD-3-Clause
# Copyright 2016-2021, Intel Corporation

#
# pull-or-rebuild-image.sh - rebuilds the Docker image used in the
#		current build (if necessary) or pulls it from the Container Registry.
#		Docker image is tagged as described in docker/build-image.sh,
#		but IMG_VER defaults in this script to "latest" (just in case it's
#		used locally without building any images).
#
# If Docker was rebuilt and all requirements are fulfilled (more details in
# push_image function below) image will be pushed to the ${CONTAINER_REG}.
#
# The script rebuilds the Docker image if:
# 1. the Dockerfile for the current OS version (${OS}-${OS_VER}.Dockerfile)
#    or any .sh script in the Dockerfiles directory were modified and committed, or
# 2. "rebuild" param was passed as a first argument to this script.
#
# The script pulls the Docker image if:
# 1. it does not have to be rebuilt (based on committed changes), or
# 2. "pull" param was passed as a first argument to this script.
#

set -e

source $(dirname ${0})/set-ci-vars.sh
IMG_VER=${IMG_VER:-latest}
TAG="${OS}-${OS_VER}-${IMG_VER}"
IMAGES_DIR_NAME=images
BASE_DIR=docker/${IMAGES_DIR_NAME}

if [[ -z "${OS}" || -z "${OS_VER}" ]]; then
	echo "ERROR: The variables OS and OS_VER have to be set properly " \
             "(eg. OS=fedora, OS_VER=34)."
	exit 1
fi

if [[ -z "${CONTAINER_REG}" ]]; then
	echo "ERROR: CONTAINER_REG environment variable is not set " \
		"(e.g. \"<registry_addr>/<org_name>/<package_name>\")."
	exit 1
fi

function build_image() {
	echo "Building the Docker image for the ${OS}-${OS_VER}.Dockerfile"
	pushd ${IMAGES_DIR_NAME}
	./build-image.sh
	popd
}

function pull_image() {
	echo "Pull the image '${CONTAINER_REG}:${TAG}' from the Container Registry."
	docker pull ${CONTAINER_REG}:${TAG}
}

function push_image {
	# Check if the image has to be pushed to the Container Registry:
	# - only upstream (not forked) repository,
	# - stable-* or master branch,
	# - not a pull_request event,
	# - and PUSH_IMAGE flag was set for current build.
	if [[ "${CI_REPO_SLUG}" == "${GITHUB_REPO}" \
		&& (${CI_BRANCH} == stable-* || ${CI_BRANCH} == master) \
		&& ${CI_EVENT_TYPE} != "pull_request" \
		&& ${PUSH_IMAGE} == "1" ]]
	then
		echo "The image will be pushed to the Container Registry: ${CONTAINER_REG}"
		pushd ${IMAGES_DIR_NAME}
		./push-image.sh
		popd
	else
		echo "Skip pushing the image to the Container Registry."
	fi
}

# If "rebuild" or "pull" are passed to the script as param, force rebuild/pull.
if [[ "${1}" == "rebuild" ]]; then
	build_image
	push_image
	exit 0
elif [[ "${1}" == "pull" ]]; then
	pull_image
	exit 0
fi

# Determine if we need to rebuild the image or just pull it from
# the Container Registry, based on committed changes.
if [ -n "${CI_COMMIT_RANGE}" ]; then
	commits=$(git rev-list ${CI_COMMIT_RANGE})
else
	commits=${CI_COMMIT}
fi

if [[ -z "${commits}" ]]; then
	echo "'commits' variable is empty. Docker image will be pulled."
fi

echo "Commits in the commit range:"
for commit in ${commits}; do echo ${commit}; done

echo "Files modified within the commit range:"
files=$(for commit in ${commits}; do git diff-tree --no-commit-id --name-only \
	-r ${commit}; done | sort -u)
for file in ${files}; do echo ${file}; done

# Check if committed file modifications require the Docker image to be rebuilt
for file in ${files}; do
	# Check if modified files are relevant to the current build
	if [[ ${file} =~ ^(${BASE_DIR})\/(${OS})-(${OS_VER})\.Dockerfile$ ]] \
		|| [[ ${file} =~ ^(${BASE_DIR})\/.*\.sh$ ]]
	then
		build_image
		push_image
		exit 0
	fi
done

# Getting here means rebuilding the Docker image isn't required (based on changed files).
# Pull the image from the Container Registry or rebuild anyway, if pull fails.
if ! pull_image; then
	build_image
	push_image
fi

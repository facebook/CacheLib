#!/bin/bash
# (c) Meta Platforms, Inc. and affiliates. Confidential and proprietary.

cd "$(hg root)" || exit 1
GEN="python3 fbcode/opensource/fbcode_builder/getdeps.py generate-github-actions"

$GEN eden --allow-system-packages --no-facebook-internal --output-dir fbcode/eden/oss/.github/workflows --os-type linux --job-file-prefix edenfs_ --job-name-prefix "EdenFS " --free-up-disk --no-tests
$GEN edencommon --allow-system-packages --output-dir fbcode/eden/common/oss/.github/workflows --os-type linux --job-file-prefix edencommon_ --job-name-prefix "EdenCommon " --free-up-disk
$GEN fb303 --allow-system-packages --free-up-disk --output-dir fbcode/fb303/github/.github/workflows --os-type linux
$GEN fbthrift --allow-system-packages --free-up-disk --output-dir fbcode/thrift/public_tld/.github/workflows
$GEN fbthrift --allow-system-packages --free-up-disk --output-dir xplat/thrift/public_tld/.github/workflows
# This manifest is no longer available. The repo has been archived.
# $GEN fbzmq --output-dir fbcode/fbzmq/public_tld/.github/workflows \
#     --disallow-system-packages --run-on-all-branches
$GEN fizz --allow-system-packages --output-dir fbcode/fizz/public_tld/.github/workflows
$GEN fizz --allow-system-packages --output-dir xplat/fizz/public_tld/.github/workflows
$GEN folly --allow-system-packages --free-up-disk --output-dir fbcode/folly/public_tld/.github/workflows
$GEN folly --allow-system-packages --free-up-disk --output-dir xplat/folly/public_tld/.github/workflows
$GEN katran --output-dir fbcode/katran/public_root/.github/workflows
# Generate macOS separately to account for cron scheduling
$GEN mononoke --os-type=linux --allow-system-packages --output-dir fbcode/eden/oss/.github/workflows --job-file-prefix mononoke_ --job-name-prefix "Mononoke " --free-up-disk --project-install-prefix mononoke:/
$GEN mononoke --os-type=darwin --cron='0 2 * * MON' --allow-system-packages --output-dir fbcode/eden/oss/.github/workflows --job-file-prefix mononoke_ --job-name-prefix "Mononoke " --free-up-disk
$GEN mononoke_integration --os-type=linux --allow-system-packages --output-dir fbcode/eden/oss/.github/workflows --test  --job-file-prefix mononoke-integration_ --job-name-prefix "Mononoke Integration " --num-jobs 4
$GEN mvfst --allow-system-packages --output-dir fbcode/quic/public_root/.github/workflows
$GEN mvfst --allow-system-packages --output-dir xplat/quic/public_root/.github/workflows
$GEN openr --cpu-cores 8 --output-dir fbcode/openr/public_tld/.github/workflows \
    --disallow-system-packages --run-on-all-branches
$GEN proxygen --output-dir fbcode/proxygen/public_tld/.github/workflows
$GEN rust-shed --allow-system-packages --no-facebook-internal --output-dir fbcode/common/rust/shed/public_tld/.github/workflows --os-type linux --job-file-prefix rust-shed_ --job-name-prefix "Rust Shed "
$GEN sapling --os-type=linux --allow-system-packages --output-dir fbcode/eden/oss/.github/workflows --num-jobs=16 --free-up-disk --job-name "Sapling CLI Getdeps " --job-file-prefix sapling-cli-getdeps_ --project-install-prefix sapling:/
$GEN wangle --allow-system-packages --output-dir fbcode/wangle/public_tld/.github/workflows
$GEN watchman --allow-system-packages --no-facebook-internal --output-dir fbcode/watchman/oss/.github/workflows
$GEN zstrong --allow-system-packages --main-branch dev --os-type darwin --os-type linux --ubuntu-version 20.04 --output-dir fbcode/data_compression/experimental/zstrong/.github/workflows

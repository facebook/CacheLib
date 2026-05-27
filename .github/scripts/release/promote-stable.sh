#!/usr/bin/env bash
# Copyright (c) Meta Platforms, Inc. and affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Force-moves the `stable` git tag to the given candidate tag and refreshes the
# corresponding GitHub Release. Idempotent: re-running with the same candidate
# is a no-op for refs (the new tag points at the same commit).
#
# Usage: promote-stable.sh <candidate-tag>
# Required env: GH_TOKEN (used implicitly by `gh`).
# Optional env: DRY_RUN=true  → print actions, mutate nothing.
set -euo pipefail

candidate=${1:?candidate tag required}
GH_REPO=facebook/CacheLib

run() {
  if [[ "${DRY_RUN:-false}" == "true" ]]; then
    echo "[DRY-RUN] $*"
  else
    "$@"
  fi
}

sha=$(git rev-list -n1 "refs/tags/${candidate}")
echo "[INFO] promoting ${candidate} (${sha}) to stable"

run git tag -f stable "${candidate}"
run git push --force origin refs/tags/stable

notes="Promoted from ${candidate} on $(date -u +%Y-%m-%dT%H:%M:%SZ)."
if gh api "repos/${GH_REPO}/releases/tags/stable" >/dev/null 2>&1; then
  run gh release edit stable --notes "${notes}"
else
  run gh release create stable --target "${sha}" --title "stable" --notes "${notes}"
fi

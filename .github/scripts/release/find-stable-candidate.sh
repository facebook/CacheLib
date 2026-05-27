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

# Picks the newest release tag eligible to become `stable`.
#
# Rule: newest release that is
#   (a) older than 1 day  (bake window — temporarily shortened from 14 days to
#       bootstrap the first `stable` release; will be restored to 14 in a
#       follow-up once `stable` exists),
#   (b) newer than the current `stable` release (or any release, if none),
#   (c) whose commit's most recent `getdeps_linux.yml` run concluded `success`
#       (latest, not "any historical pass" — so a later flake/retry that lands
#       on failure correctly disqualifies the candidate).
#
# Emits `candidate=<tag>` on stdout (suitable for `>> $GITHUB_OUTPUT`) or
# `candidate=` when nothing qualifies. All progress goes to stderr.
#
# Required env: GH_TOKEN (used implicitly by `gh`).
set -euo pipefail

BAKE_SECONDS=$(( 1 * 86400 ))
MAX_CANDIDATES=10
GH_REPO=facebook/CacheLib

current_stable_ts=""
if stable_json=$(gh api "repos/${GH_REPO}/releases/tags/stable" 2>/dev/null); then
  current_stable_ts=$(jq -r '.created_at' <<<"$stable_json")
  echo "[INFO] current stable created_at=${current_stable_ts}" >&2
else
  echo "[INFO] no existing stable release" >&2
fi

candidates=$(gh api --paginate "repos/${GH_REPO}/releases" \
  | jq -r --arg stable "$current_stable_ts" --argjson bake "$BAKE_SECONDS" '
      .[] | select(
        (now - (.created_at | fromdateiso8601)) > $bake
        and (($stable == "") or (.created_at > $stable))
      ) | .tag_name
  ' | head -n "$MAX_CANDIDATES")

if [[ -z "$candidates" ]]; then
  echo "[INFO] no candidate in window" >&2
  echo "candidate="
  exit 0
fi

for tag in $candidates; do
  sha=$(git rev-list -n1 "refs/tags/${tag}")
  echo "[INFO] checking CI status for ${tag} (${sha})" >&2
  latest_linux=$(gh api \
    "repos/${GH_REPO}/actions/workflows/getdeps_linux.yml/runs?head_sha=${sha}&per_page=1" \
    | jq -r '.workflow_runs[0].conclusion // "missing"')
  echo "[INFO] latest linux conclusion for ${tag}: ${latest_linux}" >&2
  if [[ "$latest_linux" == "success" ]]; then
    echo "[PASS] ${tag}" >&2
    echo "candidate=${tag}"
    exit 0
  fi
  echo "[FAIL] ${tag} (latest linux conclusion: ${latest_linux})" >&2
done

echo "[WARN] no candidate passed validation" >&2
echo "candidate="

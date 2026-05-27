# Release scripts

Helpers invoked by `.github/workflows/release.yml` to maintain the `stable` tag.

| Script | Purpose |
|--------|---------|
| `find-stable-candidate.sh` | Pick the newest release tag eligible to become `stable` (older than 1 day during bootstrap — will be raised to 14 days once `stable` exists; newer than current stable; with a successful `linux` workflow run). Prints `candidate=<tag>` (or `candidate=`) to stdout. |
| `promote-stable.sh <tag>` | Force-move the `stable` git tag to `<tag>` and refresh the `stable` GitHub Release. Honors `DRY_RUN=true`. |

## Local dry-run

Both scripts hardcode `facebook/CacheLib` and only need `gh` to be authenticated:

```bash
./find-stable-candidate.sh                       # prints to stdout
DRY_RUN=true ./promote-stable.sh v2024.05.06     # prints what it would do
```

`promote-stable.sh` without `DRY_RUN=true` will move tags on `origin` — only run it locally if you are intentionally bypassing the workflow.

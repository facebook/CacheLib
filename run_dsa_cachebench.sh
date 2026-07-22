#!/bin/bash
# Build CacheLib with DTO, configure DSA work queues, and run cachebench
# pinned to socket 0 with DSA checksum offload and (optionally) DTO's
# transparent memcpy/memset interception.
#
# Usage: ./run_dsa_cachebench.sh [options]
#   --workload cdn|bigcache
#                       cdn      = synthetic CDN hit-ratio config (default)
#                       bigcache = production BigCache trace replay
#                                  (~/bigcache_trace_sea1c01_20250414_20250421.json,
#                                  scaled exactly as the offload study: 400GB Navy,
#                                  16GB parcel, 500k ops/thread, fiber scheduler)
#   --config PATH       base cachebench config (overrides the workload default)
#   --offload on|off|none
#                       on   = Navy data checksums on, computed by DSA
#                       off  = Navy data checksums on, computed in software
#                       none = base config's own checksum setting
#                              (cdn: no Navy section added; bigcache: checksums
#                              off = the study's no-checksum baseline A)
#   --intercept on|off  transparent DTO interception of large memcpy/memset.
#                       off (default) sets DTO_MIN_BYTES=1GB so only the explicit
#                       checksum-offload API uses DSA; on leaves DTO's own
#                       size threshold (32KB) in effect.
#   --fibers on|off     Navy fiber scheduler (NavyRequestScheduler, libaio,
#                       study settings: maxNumReads 1024 / maxNumWrites 1200 /
#                       qDepth 32). Default off; pass --fibers on to enable.
#   --nvm-size-mb N     Navy cache size (default: cdn 20480, bigcache 409600)
#   --num-ops N         override test_config.numOps (per stressor thread;
#                       bigcache default 500000 = the study's 12M-op replay)
#   --devices "0 2 4 6" DSA device ids to enable (default "0 2 4 6")
#   --out DIR           output/work directory (default ./dsa_bench_out)
#   --skip-build        do not (re)build cachebench
#   --skip-dsa          do not reconfigure DSA work queues
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$REPO/build-cachelib"
PREFIX="$REPO/opt/cachelib"
DTO_DIR="${DTO_DIR:-$HOME/DTO}"

WORKLOAD=cdn
CONFIG=""
OFFLOAD=on
INTERCEPT=off
FIBERS=off
NVM_SIZE_MB=""
NUM_OPS=""
DEVICES="0 2 4 6"
OUT="$PWD/dsa_bench_out"
SKIP_BUILD=0
SKIP_DSA=0

while [ $# -gt 0 ]; do
  case "$1" in
    --workload)    WORKLOAD="$2"; shift 2 ;;
    --config)      CONFIG="$2"; shift 2 ;;
    --offload)     OFFLOAD="$2"; shift 2 ;;
    --intercept)   INTERCEPT="$2"; shift 2 ;;
    --fibers)      FIBERS="$2"; shift 2 ;;
    --nvm-size-mb) NVM_SIZE_MB="$2"; shift 2 ;;
    --num-ops)     NUM_OPS="$2"; shift 2 ;;
    --devices)     DEVICES="$2"; shift 2 ;;
    --out)         OUT="$2"; shift 2 ;;
    --skip-build)  SKIP_BUILD=1; shift ;;
    --skip-dsa)    SKIP_DSA=1; shift ;;
    -h|--help)     sed -n '2,33p' "$0"; exit 0 ;;
    *) echo "unknown option: $1" >&2; exit 1 ;;
  esac
done

case "$WORKLOAD" in
  cdn)
    [ -n "$CONFIG" ] || CONFIG="$REPO/cachelib/cachebench/test_configs/hit_ratio/cdn/config.json"
    [ -n "$NVM_SIZE_MB" ] || NVM_SIZE_MB=20480
    ;;
  bigcache)
    [ -n "$CONFIG" ] || CONFIG="$HOME/bigcache_trace_sea1c01_20250414_20250421.json"
    [ -n "$NVM_SIZE_MB" ] || NVM_SIZE_MB=409600
    [ -n "$NUM_OPS" ] || NUM_OPS=500000
    ;;
  *) echo "--workload must be cdn|bigcache" >&2; exit 1 ;;
esac
case "$OFFLOAD" in on|off|none) ;; *) echo "--offload must be on|off|none" >&2; exit 1 ;; esac
case "$INTERCEPT" in on|off) ;; *) echo "--intercept must be on|off" >&2; exit 1 ;; esac
case "$FIBERS" in on|off) ;; *) echo "--fibers must be on|off" >&2; exit 1 ;; esac
mkdir -p "$OUT"

export LD_LIBRARY_PATH="$PREFIX/lib:/usr/local/lib:${LD_LIBRARY_PATH:-}"

# ---------------------------------------------------------------- build ----
if [ "$SKIP_BUILD" = 0 ]; then
  echo "== Building cachebench (BUILD_WITH_DTO=ON) =="
  if [ ! -f "$BUILD_DIR/CMakeCache.txt" ]; then
    mkdir -p "$BUILD_DIR"
    export CMAKE_PREFIX_PATH="$PREFIX/lib/cmake:$PREFIX:/usr/local${CMAKE_PREFIX_PATH:+:$CMAKE_PREFIX_PATH}"
    # CMAKE_DISABLE_FIND_PACKAGE_uring: cachelib enables its io_uring path
    # whenever liburing exists on the system, but that requires a folly
    # built with liburing; disable the probe so the two cannot disagree.
    cmake -S "$REPO/cachelib" -B "$BUILD_DIR" \
      -DCMAKE_INSTALL_PREFIX="$PREFIX" \
      -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DBUILD_TESTS=ON \
      -DBUILD_WITH_DTO=ON \
      -DCMAKE_DISABLE_FIND_PACKAGE_uring=ON
  fi
  cmake --build "$BUILD_DIR" --target cachebench -j "$(nproc)"
fi
CACHEBENCH="$BUILD_DIR/cachebench/cachebench"
[ -x "$CACHEBENCH" ] || { echo "cachebench not found at $CACHEBENCH" >&2; exit 1; }

# ------------------------------------------------------------ DSA setup ----
if [ "$SKIP_DSA" = 0 ]; then
  echo "== Configuring DSA shared work queues (devices: $DEVICES) =="
  for d in $DEVICES; do
    sudo "$DTO_DIR/accelConfig.sh" "$d" yes 0 4 >/dev/null
  done
  sudo chmod 0666 /dev/dsa/wq*.0
fi
ls /dev/dsa/wq*.0 >/dev/null 2>&1 || { echo "no enabled DSA WQs under /dev/dsa" >&2; exit 1; }
echo "DSA WQs: $(ls /dev/dsa/)"

# ------------------------------------------------------- derive config -----
RUN_CONFIG="$OUT/config_${WORKLOAD}_offload_${OFFLOAD}.json"
CONFIG="$CONFIG" RUN_CONFIG="$RUN_CONFIG" OFFLOAD="$OFFLOAD" \
NVM_SIZE_MB="$NVM_SIZE_MB" NUM_OPS="$NUM_OPS" OUT="$OUT" FIBERS="$FIBERS" \
python3 - <<'EOF'
import json, os, shutil, sys
cfgPath = os.environ["CONFIG"]
cfg = json.load(open(cfgPath))
base = os.path.dirname(os.path.abspath(cfgPath))
tc = cfg["test_config"]
cc = cfg["cache_config"]
offload = os.environ["OFFLOAD"]
isTraceReplay = "replay" in tc.get("generator", "")

# cachebench resolves distribution files by prepending the config file's
# directory (even to absolute paths), so copy them next to the derived
# config and reference them by basename.
for k in ("popDistFile", "valSizeDistFile"):
    if k in tc:
        src = tc[k] if os.path.isabs(tc[k]) else os.path.join(base, tc[k])
        dst = os.path.join(os.environ["OUT"], os.path.basename(src))
        if os.path.abspath(src) != os.path.abspath(dst):
            shutil.copyfile(src, dst)
        tc[k] = os.path.basename(src)
# the (multi-GB) trace file is opened directly: absolutize, don't copy
if "traceFileName" in tc and not os.path.isabs(tc["traceFileName"]):
    tc["traceFileName"] = os.path.join(base, tc["traceFileName"])
if "traceFileName" in tc and not os.path.exists(tc["traceFileName"]):
    sys.exit(f"trace file not found: {tc['traceFileName']}")

if os.environ["NUM_OPS"]:
    tc["numOps"] = int(os.environ["NUM_OPS"])

if isTraceReplay:
    # Trace replay (BigCache): the base config already has a Navy section
    # sized for its original production host; rescale it to this machine
    # exactly as the offload study did.
    cc["nvmCacheSizeMB"] = int(os.environ["NVM_SIZE_MB"])
    cc["nvmCachePaths"] = [os.path.join(os.environ["OUT"], "navy_cache_file")]
    cc["navyParcelMemoryMB"] = 16384
    if offload != "none":
        cc["navyDataChecksum"] = True
        cc["navyChecksumOffload"] = offload == "on"
        cc["navyChecksumOffloadMinSize"] = 4096
elif offload != "none":
    # Synthetic workloads (CDN): the base config is DRAM-only; add the
    # hybrid Navy tier the offload applies to.
    cc["nvmCacheSizeMB"] = int(os.environ["NVM_SIZE_MB"])
    cc["nvmCachePaths"] = [os.path.join(os.environ["OUT"], "navy_cache_file")]
    cc["navyReaderThreads"] = 32
    cc["navyWriterThreads"] = 32
    cc["navyDataChecksum"] = True
    cc["navyChecksumOffload"] = offload == "on"
    cc["navyChecksumOffloadMinSize"] = 4096

if os.environ["FIBERS"] == "on":
    # NavyRequestScheduler (fibers, libaio) with the study's settings;
    # required for the offload's yield-during-DSA-wait to overlap work.
    cc["navyMaxNumReads"] = 1024
    cc["navyMaxNumWrites"] = 1200
    cc["navyQDepth"] = 32
    cc["navyEnableIoUring"] = False

json.dump(cfg, open(os.environ["RUN_CONFIG"], "w"), indent=2)
print(f"wrote {os.environ['RUN_CONFIG']}")
EOF

# ---------------------------------------------------------------- run ------
export DTO_USESTDC_CALLS=0
export DTO_CRC_MIN_BYTES=4096
export DTO_WAIT_METHOD="${DTO_WAIT_METHOD:-busypoll}"
export DTO_COLLECT_STATS=1
if [ "$INTERCEPT" = off ]; then
  export DTO_MIN_BYTES=1073741824   # 1GB: transparent interception off
else
  unset DTO_MIN_BYTES               # DTO's own threshold decides
fi

LOG="$OUT/cachebench_${WORKLOAD}_offload_${OFFLOAD}_intercept_${INTERCEPT}.log"
echo "== Running cachebench on socket 0 (workload=$WORKLOAD offload=$OFFLOAD intercept=$INTERCEPT fibers=$FIBERS) =="
echo "   log: $LOG"

# The fiber scheduler has a known ~1-in-4 startup hang (frozen at
# NavyRequestDispatcher startup). With --progress 60 a healthy run writes to
# its log at least once a minute, so 150s of log silence while the process
# lives means hung: kill and retry (up to 3 attempts).
attempt_run() {
  rm -f "$OUT/navy_cache_file"
  /usr/bin/time -v numactl -N 0 \
    "$CACHEBENCH" --json_test_config "$RUN_CONFIG" --progress 60 \
    > "$LOG" 2>&1 &
  local tpid=$!
  local last=-1 silent=0
  while kill -0 "$tpid" 2>/dev/null; do
    sleep 30
    local sz
    sz=$(stat -c %s "$LOG" 2>/dev/null || echo 0)
    if [ "$sz" = "$last" ]; then
      silent=$((silent + 1))
      if [ "$silent" -ge 5 ]; then
        pkill -9 -P "$tpid" 2>/dev/null
        kill -9 "$tpid" 2>/dev/null
        wait "$tpid" 2>/dev/null
        return 99
      fi
    else
      silent=0
    fi
    last=$sz
  done
  wait "$tpid"
}

set +e
rc=99
for attempt in 1 2 3; do
  attempt_run
  rc=$?
  [ "$rc" -ne 99 ] && break
  echo "WATCHDOG: run frozen for 150s+ (startup hang?), retrying (attempt $attempt of 3)"
done
set -e
rm -f "$OUT/navy_cache_file"

# ------------------------------------------------------------- report ------
echo "exit=$rc"
grep -E "Total Ops|get   |set   |NVM Gets|NVM Puts" "$LOG" | head -6 || true
echo "checksum error lines: $(grep -ciE 'checksum.*(error|mismatch)' "$LOG" || true)"
grep -E "User time|System time|Elapsed \(wall|Maximum resident" "$LOG" || true
if grep -q "Number of Memory Operations" "$LOG"; then
  echo "-- DTO op counts (see full table in the log) --"
  grep -A20 "Number of Memory Operations" "$LOG" | head -24
fi
exit $rc

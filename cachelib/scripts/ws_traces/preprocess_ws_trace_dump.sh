#!/bin/bash
# Run this script from the directory that has the log dumps from warm storage.

if [[ $# -ne 1 ]]; then 
    echo "Usage: $0 <dir>"
    exit 1
fi

TRACE_DIR="$1"
OUT_FILE="$TRACE_DIR/raw_all_features.trace"
COMPRESSED_OUT_FILE="$OUT_FILE.gz"

# check if the file already exists
if test -f "$COMPRESSED_OUT_FILE"; then
    echo "Already processed. $COMPRESSED_OUT_FILE exists"
    exit 0
fi

# combine them into one large trace
gunzip -c "$TRACE_DIR"/processed.stderr*.gz > "$OUT_FILE" || exit 1

buck run //cachelib/scripts/ws_traces:sample_trace "$OUT_FILE" 1.0 &
buck run //cachelib/scripts/ws_traces:sample_trace "$OUT_FILE" 10.0 &
buck run //cachelib/scripts/ws_traces:sample_trace "$OUT_FILE" 20.0 &
buck run //cachelib/scripts/ws_traces:sample_trace "$OUT_FILE" 100.0 &
wait

# compress the concatenated file and remove the raw ones
gzip "$OUT_FILE" || exit 1

# remove the input compressed files
\rm "$TRACE_DIR"/processed.stderr*.gz

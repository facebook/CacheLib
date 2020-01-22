from __future__ import absolute_import, division, print_function, unicode_literals

import os
import sys
from optparse import OptionParser

import cachelib.scripts.ws_traces.cache as cache
import cachelib.scripts.ws_traces.utils as utils


def compute_cache_stats(a, cache_lifetime, reject_first, fifo):
    stats = {}
    for k in a:
        stats[k] = cache.CacheStats(reject_first, fifo)
        stats[k].sim_cache(a[k].accesses, cache_lifetime)
    return stats


parser = OptionParser()
parser.add_option(
    "", "--fifo", dest="fifo", help="Simulate fifo", action="store_true", default=False
)

parser.add_option(
    "",
    "--reject-first",
    dest="reject_first",
    help="Simulate reject first",
    action="store_true",
    default=False,
)

parser.add_option(
    "-o",
    "--output-dir",
    dest="output_dir",
    help="Destination directory for results",
    default="",
)

(options, args) = parser.parse_args()


tracefile = args[0]
if not options.output_dir:
    options.output_dir = "/".join(tracefile.split("/")[:-1])

options.output_dir += utils.get_output_suffix(options)

# create the output directory
os.makedirs(options.output_dir, 0o755, exist_ok=True)

input_file_name = tracefile[: -len(".trace")].split("/")[-1]
out_file_name = "{}/{}_reuse_stats.txt".format(options.output_dir, input_file_name)
a, start_ts, end_ts = utils.read_processed_file(tracefile)
trace_duration_secs = end_ts - start_ts
sampling_ratio = float(input_file_name.split(".")[0].split("_")[-1])

cache_lifetime_hrs = [0.1, 0.5, 1, 2, 5, 6, 12, 24, 32, 36, 40, 48, 0]

fields = [
    "Life-Hrs",
    "ChunkHitRate",
    "%IOPS-Saved",
    "MB/s",
    "CacheableMB/s",
    "%FullCache-IOPS",
    "%LoneAccess-IOPS",
    "%K-IOPS-Saved",
    "%K-FullCache",
    "%K-NonCacheable",
    "%K-LoneAccess",
]

print_fmt_hdr, print_fmt_line = utils.make_format_string(fields)

print("Trace duration {} secs".format(trace_duration_secs))
with open(out_file_name, "w+") as out:
    print("Simulation options: {}".format(options), file=out)
    print("Chunk-Size: ", utils.BlkAccess.ALIGNMENT, file=out)
    print(print_fmt_hdr.format(fields), file=out)

    total_iops = None
    for t in cache_lifetime_hrs:
        print("Computing for {} hrs".format(t))
        stats = compute_cache_stats(
            a, int(t * 3600), options.reject_first, options.fifo
        )

        def pct_k(x):
            return utils.pct(x, len(a))

        k_single_access = sum(1 for _, x in stats.items() if x.isSingleAccess())
        k_no_hits = sum(1 for _, x in stats.items() if x.isNonCacheable())
        chunks_cacheable = sum(
            x.numChunksWritten() for _, x in stats.items() if x.numHits() > 0
        )
        k_full_hits = sum(1 for _, x in stats.items() if x.isFullCacheable())
        full_hits_iops = sum(
            x.totalAccess() for _, x in stats.items() if x.isFullCacheable()
        )
        k_iops_saved = sum(1 for _, x in stats.items() if x.numHits() > 0)
        iops_saved = sum(x.numHits() for _, x in stats.items())
        if total_iops is None:
            total_iops = sum(x.totalAccess() for _, x in stats.items())

        total_chunks = sum(x.numChunksAccessed() for _, x in stats.items())
        hit_chunks = sum(x.numChunksHit() for _, x in stats.items())
        chunks_written = sum(x.numChunksWritten() for _, x in stats.items())

        write_mb_per_sec = utils.mb_per_sec(
            chunks_written, trace_duration_secs, sampling_ratio
        )
        cacheable_write_mb_per_sec = utils.mb_per_sec(
            chunks_cacheable, trace_duration_secs, sampling_ratio
        )

        print(
            print_fmt_line.format(
                t,
                utils.pct(hit_chunks, total_chunks),
                utils.pct(iops_saved, total_iops),
                write_mb_per_sec,
                cacheable_write_mb_per_sec,
                utils.pct(full_hits_iops, total_iops),
                utils.pct(k_single_access, total_iops),
                pct_k(k_iops_saved),
                pct_k(k_full_hits),
                pct_k(k_no_hits),
                pct_k(k_single_access),
            ),
            file=out,
        )
    print("Total IOPS = {}".format(total_iops))

print("Generated output in ", out_file_name)

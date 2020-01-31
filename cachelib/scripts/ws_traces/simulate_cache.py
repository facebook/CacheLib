from __future__ import absolute_import, division, print_function, unicode_literals

import os
from optparse import OptionParser

import cachelib.scripts.ws_traces.sim_cache as sim_cache
import cachelib.scripts.ws_traces.utils as utils


def simulate_cache(cache, accesses):
    iops_saved = 0

    for a in accesses:
        blk = a[0]
        miss = False
        for c in a[1].chunks():
            k = (blk, c)
            found = cache.find(k, a[1].ts)
            if not found:
                cache.insert(k, a[1].ts, a[1].features.toList())
                miss = True

        if not miss:
            iops_saved += 1
    return iops_saved


parser = OptionParser()

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

parser.add_option(
    "",
    "--perm",
    dest="perm",
    help="Read features if present and simulate only perm",
    action="store_true",
    default=False,
)

parser.add_option(
    "",
    "--temp",
    dest="temp",
    help="Read features if present and simulate only temp",
    action="store_true",
    default=False,
)

parser.add_option(
    "", "--fifo", dest="fifo", help="Simulate fifo", action="store_true", default=False
)

parser.add_option(
    "",
    "--lirs",
    dest="lirs",
    help="Enable lirs instead of LRU/FIFO",
    action="store_true",
)

parser.add_option(
    "-w",
    "--write_mbps",
    dest="write_mbps",
    help="Expected write rate MB/sec",
    action="store",
    default=0,
)

parser.add_option(
    "-s",
    "--write_mbps_scale_size_gb",
    dest="write_mbps_scale_size_gb",
    help="The cache capacity to scale the write mbps. By default we take the "
    "write mbps of the current WS HW sku ",
    action="store",
    default=600,
)


(options, args) = parser.parse_args()

if options.fifo and options.lirs:
    "Can not run with both fifo and lirs"
    os.exit(1)

use_lru = not (options.fifo or options.lirs)
assert use_lru or options.lirs or options.fifo

tracefile = args[0]
if not options.output_dir:
    options.output_dir = "/".join(tracefile.split("/")[:-1])

options.output_dir += utils.get_output_suffix(options)

# create the output directory
os.makedirs(options.output_dir, 0o755, exist_ok=True)

input_file_name = tracefile[: -len(".trace")].split("/")[-1]
out_file_name = "{}/{}_cache_perf.txt".format(options.output_dir, input_file_name)
accesses, start_ts, end_ts = utils.read_processed_file_list_accesses(tracefile)

trace_duration_secs = round(end_ts - start_ts, 2)
sampling_ratio = float(input_file_name.split(".")[0].split("_")[-1])

# filter out just get accesses
valid_op_types = [utils.OpType.GET_PERM, utils.OpType.GET_TEMP]
if options.perm and not options.temp:
    valid_op_types = [utils.OpType.GET_PERM]
elif options.temp and not options.perm:
    valid_op_types = [utils.OpType.GET_TEMP]

accesses = [a for a in accesses if a[1].features and a[1].features.op in valid_op_types]

fields = [
    "SizeGB",
    "#Elems",
    "%IOPS-Saved",
    "MB/s",
    "#Evictions",
    "AvgEvictionAge",
    "#NoHitEvits",
    "AvgNoHitEvictAge",
]
print_fmt_hdr, print_fmt_line = utils.make_format_string(fields)

sizes_gb = [300, 600, 1024, 1500, 2048, 3072, 4096, 8192, 10240, 16384]
total_iops = len(accesses)

max_key = max(accesses, key=lambda x: x[0])

scaled_write_mbps = float(options.write_mbps) * float(sampling_ratio) / 100.0

with open(out_file_name, "w+") as out:
    print("Simulation options: {}".format(options), file=out)
    print("Chunk-Size: ", utils.BlkAccess.ALIGNMENT, file=out)
    print("Total IOPS = {}".format(total_iops), file=out)
    print("#Blks = {}".format(max_key[0]), file=out)
    print("Trace duration {} secs\n\n".format(trace_duration_secs), file=out)
    print(print_fmt_hdr.format(fields), file=out)

    for size_gb in sizes_gb:
        num_cache_elems = (
            size_gb * 1024 * 1024 * 1024 * sampling_ratio / 100
        ) // utils.BlkAccess.ALIGNMENT

        cache_size_write_rate_factor = size_gb / float(options.write_mbps_scale_size_gb)
        write_mbps = scaled_write_mbps * cache_size_write_rate_factor

        ap = None
        if options.reject_first:
            if write_mbps == 0:
                ap = sim_cache.RejectFirstAP(2 * num_cache_elems)
            else:
                ap = sim_cache.RejectFirstWriteRateAP(
                    2 * num_cache_elems, write_mbps, utils.BlkAccess.ALIGNMENT
                )
        else:
            if write_mbps != 0:
                ap = sim_cache.WriteRateRejectAP(write_mbps, utils.BlkAccess.ALIGNMENT)
            else:
                ap = sim_cache.AcceptAll()

        cache = None
        if options.lirs:
            cache = sim_cache.LIRSCache(num_cache_elems, 1.0, ap)
        else:
            cache = sim_cache.QueueCache(use_lru, num_cache_elems, ap)

        iops_saved = simulate_cache(cache, accesses)
        chunks_written = cache.keys_written

        write_mb_per_sec = utils.mb_per_sec(
            chunks_written, trace_duration_secs, sampling_ratio
        )

        print(
            print_fmt_line.format(
                size_gb,
                num_cache_elems,
                utils.pct(iops_saved, total_iops),
                write_mb_per_sec,
                cache.evictions,
                cache.computeEvictionAge(),
                cache.un_accessed_evictions,
                cache.computeNoHitEvictionAge(),
            ),
            file=out,
        )
print("Generated output in ", out_file_name)

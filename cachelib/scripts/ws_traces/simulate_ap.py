#!/usr/bin/env python3

import json
import os
from optparse import OptionParser

import cachelib.scripts.ws_traces.sim_cache as sim_cache
import cachelib.scripts.ws_traces.utils as utils


"""
This module simulates different combinations of eviction policies and admission
policies as described below -

Part 1: Eviction Policy

1. lru                  default
2. fifo                 --fifo
3. lirs                 --lirs

Part 2: admission policy
1. WriteRateRejectAP    defualt         (write_mbps <> 0)
2. AcceptAll            defualt         (write_mbps == 0)
3. RejectXAP            --rejectx_ap   (ap_threshold, ap_probability, write_mbps)
4. LearnedAP            --learned_ap   (learned_ap_model, ap_threshold,
                                        learned_ap_filter_count)
                                        (not applicable to lirs)
5. CoinFlipAP           --coinflip_ap  (ap_probability)

Major difference from simulate_cache -

1. remove loops around cache size (default to 600)
2. add more options around admission and eviction policies
3. add intermediate stats for better debugging purposes and early results preview
"""


def main(options, args):
    print(options)
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

    accesses = [
        (block_id, access)
        for block_id, access in accesses
        if access.features and access.features.op in valid_op_types
    ]

    total_iops = len(accesses)

    scaled_write_mbps = float(options.write_mbps) * float(sampling_ratio) / 100.0

    max_key = max(accesses, key=lambda x: x[0])

    # output is formatted as json from the following dict
    logjson = {}
    logjson["options"] = vars(options)
    logjson["chunkSize"] = utils.BlkAccess.ALIGNMENT
    logjson["totalIOPS"] = total_iops
    logjson["blkCount"] = max_key[0]
    logjson["traceSeconds"] = trace_duration_secs

    if options.lirs:
        logjson["EvictionPolicy"] = "LIRS"
    elif options.fifo:
        logjson["EvictionPolicy"] = "FIFO"
    else:
        logjson["EvictionPolicy"] = "LRU"

    logjson["samplingRatio"] = sampling_ratio

    logjson["results"] = {}

    num_cache_elems = (
        options.size_gb * 1024 * 1024 * 1024 * sampling_ratio / 100
    ) // utils.BlkAccess.ALIGNMENT

    ap = None
    apname = ""
    if options.rejectx_ap:
        threshold = 1
        factor = 2
        if options.ap_threshold:
            threshold = options.ap_threshold
        if options.ap_probability:
            factor = options.ap_probability
        if scaled_write_mbps == 0:
            ap = sim_cache.RejectXAP(threshold, factor * num_cache_elems)
            apname = "RejectX"
        else:
            ap = sim_cache.RejectFirstWriteRateAP(
                2 * num_cache_elems, scaled_write_mbps, utils.BlkAccess.ALIGNMENT
            )
            apname = "RejectFirstWriteRate"
    elif options.learned_ap:
        assert options.learned_ap_model and options.ap_threshold
        ap = sim_cache.LearnedAP(options.learned_ap_model, options.ap_threshold)
        print(
            "Learned AP with model:",
            options.learned_ap_model,
            "threshold:",
            options.ap_threshold,
        )
        apname = "Learned-Regression"
    elif options.coinflip_ap:
        assert options.ap_probability
        ap = sim_cache.CoinFlipAP(options.ap_probability)
        print("CoinFlip AP with probability:", options.ap_probability)
        apname = "CoinFlip-P"

    else:
        if scaled_write_mbps != 0:
            ap = sim_cache.WriteRateRejectAP(
                scaled_write_mbps, utils.BlkAccess.ALIGNMENT
            )
            apname = "WriteRateReject"
        else:
            ap = sim_cache.AcceptAll()
            apname = "AcceptAll"

    logjson["AdmissionPolicy"] = apname

    if options.lirs:
        cache = sim_cache.LIRSCache(num_cache_elems, 1.0, ap)
    else:
        cache = sim_cache.QueueCache(
            use_lru, num_cache_elems, ap, options.learned_ap_filter_count
        )

    stats = sim_cache.simulate_cache(cache, accesses, sampling_ratio)

    chunks_written = cache.keys_written
    write_mb_per_sec = utils.mb_per_sec(
        chunks_written, trace_duration_secs, sampling_ratio
    )
    total_iops_saved = sum(stats["iops_saved"])
    logjson["results"]["NumCacheElems"] = num_cache_elems
    logjson["results"]["IOPSRequested"] = stats["iops_requests"]
    logjson["results"]["IOPSSaved"] = stats["iops_saved"]
    logjson["results"]["TotalIOPSSaved"] = total_iops_saved
    logjson["results"]["ChunkQueries"] = stats["chunk_queries"]
    logjson["results"]["ChunkHits"] = stats["chunk_hits"]
    logjson["results"]["IOPsPartialHits"] = stats["iops_partial_hits"]
    logjson["results"]["FlashWriteRate"] = write_mb_per_sec
    logjson["results"]["NumCacheEviction"] = cache.evictions
    logjson["results"]["AvgEvictionAge"] = cache.computeEvictionAge()
    logjson["results"]["NumNoHitEvictions"] = cache.un_accessed_evictions
    logjson["results"]["AvgNoHitEvictionAge"] = cache.computeNoHitEvictionAge()
    logjson["results"]["ChunkWritten"] = stats["chunks_written"]
    logjson["results"]["Duration"] = stats["duration"]

    print(
        f"Results preview: \n \
        Flash write rate - {write_mb_per_sec} MB/s \n \
        IOPS saved ratio - {total_iops_saved / total_iops}"
    )
    with open(out_file_name, "w+") as out:
        json.dump(logjson, out)


def add_options_parser():
    parser = OptionParser()

    parser.add_option(
        "",
        "--fifo",
        dest="fifo",
        help="Simulate fifo",
        action="store_true",
        default=False,
    )

    parser.add_option(
        "",
        "--lirs",
        dest="lirs",
        help="Enable lirs instead of LRU/FIFO",
        action="store_true",
        default=False,
    )

    parser.add_option(
        "",
        "--rejectx-ap",
        dest="rejectx_ap",
        help="Simulate reject x admission policy",
        action="store_true",
        default=False,
    )

    parser.add_option(
        "",
        "--learned-ap",
        dest="learned_ap",
        help="Simulate with a learned admission policy",
        action="store_true",
        default=False,
    )

    parser.add_option(
        "",
        "--coinflip-ap",
        dest="coinflip_ap",
        help="Simulate with a coin flip admission policy",
        action="store_true",
        default=False,
    )

    parser.add_option(
        "",
        "--ap-threshold",
        dest="ap_threshold",
        help="Set the admission policy's threshold",
        action="store",
        type="float",
    )

    parser.add_option(
        "",
        "--ap-probability",
        dest="ap_probability",
        help="Set the admission probability",
        action="store",
        type="float",
    )

    parser.add_option(
        "",
        "--learned-ap-model",
        dest="learned_ap_model",
        help="Set the file that stores the learned admission policy's model",
        action="store",
        type="string",
    )

    parser.add_option(
        "",
        "--learn-ap-filtercount",
        dest="learned_ap_filter_count",
        help="Set the number of bloom filters for history features",
        action="store",
        type="int",
        default=12,
    )

    parser.add_option(
        "-o",
        "--output-dir",
        dest="output_dir",
        help="Destination directory for results",
        default="",
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
        "--size_gb",
        dest="size_gb",
        help="Cache size in GB",
        action="store",
        type="int",
        default=600,
    )

    (options, args) = parser.parse_args()

    if options.fifo and options.lirs:
        "Can not run with both fifo and lirs"
        os.exit(1)
    return options, args


if __name__ == "__main__":
    (options, args) = add_options_parser()

    main(options, args)

#!/usr/bin/env python3

import os
from optparse import OptionParser

import cachelib.scripts.ws_traces.sim_cache as sim_cache


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
        default=6,
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

    sim_cache.simulate_cache_driver(options, args)

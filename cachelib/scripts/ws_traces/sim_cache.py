#!/usr/bin/env python3

import json
import os
import random
from collections import OrderedDict

import cachelib.scripts.ws_traces.dynamic_features as dyn_features
import cachelib.scripts.ws_traces.utils as utils
import lightgbm as lgb
import numpy as np


ADMIT_BUFFER_LIMIT = 512


class LIRSItem(object):
    def __init__(self, ts):
        self.last_access_time = ts
        self.hits = 0
        self.is_bait = True

    def markAccessed(self, ts):
        self.last_access_time = ts
        self.hits += 1

    def upgradeFromBait(self, ts):
        assert self.is_bait
        if self.is_bait:
            self.is_bait = False
            self.hits = 0
            self.last_access_time = ts

    def isBait(self):
        return self.is_bait

    def isNotBait(self):
        return not self.is_bait


class LIRSCache(object):
    def __init__(
        self, num_elems, bait_factor, ap, filter_count, access_history_use_counts
    ):
        self.cache_size = num_elems
        self.cache = OrderedDict()

        # number of baits  in cache
        self.num_baits = 0

        # number of values in cache
        self.num_vals = 0

        # required ratio of baits to values
        self.bait_factor = bait_factor

        # to optimize bait eviction, we let more baits stay in cache and prune
        # once it reaches past this threshold.
        self.prune_ratio = 1.25

        self.ap = ap
        self.keys_written = 0
        self.rejections = 0
        self.evictions = 0
        self.eviction_age_cum = 0
        self.un_accessed_evictions = 0
        self.un_accessed_eviction_age_cum = 0

        # for ml admission policy
        self.dynamic_features = dyn_features.DynamicFeatures(
            filter_count, access_history_use_counts
        )

    def str(self):
        return "size={} vals={} baits={}".format(
            len(self.cache), self.num_vals, self.num_baits
        )

    def find(self, key, key_ts):
        found = key in self.cache
        reuse_dist = 0
        is_bait = False
        if found:
            reuse_dist = key_ts - self.cache[key].last_access_time
            assert reuse_dist >= 0
            is_bait = self.cache[key].isBait()
            self.cache[key].markAccessed(key_ts)
            self.cache.move_to_end(key)

        return found and not is_bait

    def insert(self, key, ts, keyfeaturelist=None):
        # ML ap is not supported by LIRS Cache at this point,
        # "keyfeaturelist" is just a placeholder here for api consistency
        # first time insertion introduces just the bait to track hotness
        if key not in self.cache:
            self.insert_bait(key, ts)
            return

        if not self.ap.accept(key, ts):
            self.rejections += 1
            return

        self.cache[key].upgradeFromBait(ts)
        self.cache.move_to_end(key)
        self.num_baits -= 1
        self.num_vals += 1
        self.keys_written += 1

        if self.num_vals > self.cache_size:
            self.do_lirs_eviction(ts)

    def too_many_baits(self):
        return (
            self.num_baits > 100 and self.num_baits > self.bait_factor * self.num_vals
        )

    def should_prune_baits(self):
        return self.num_baits > 100 and (
            self.num_baits > self.bait_factor * self.prune_ratio * self.num_vals
        )

    # scan from the bottom of lru and evict baits if we are above the
    # threshold. This is done lazily to ammortize the cost of baits to O(1)
    def do_lirs_bait_eviction(self):
        baits_to_remove = []
        for k in self.cache.keys():
            if self.cache[k].isBait():
                baits_to_remove.append(k)
                self.num_baits -= 1

            if not self.too_many_baits():
                break

        for k in baits_to_remove:
            self.cache.pop(k)

    def insert_bait(self, key, key_ts):
        # insert the element as bait
        self.cache[key] = LIRSItem(key_ts)
        self.num_baits += 1

        if self.should_prune_baits():
            self.do_lirs_bait_eviction()

    def remove_baits_lru_end(self):
        # eliminate all baits in the end so that inserting values is O(1)
        baits_to_remove = []
        for k in self.cache.keys():
            if self.cache[k].isNotBait():
                break
            baits_to_remove.append(k)

        for k in baits_to_remove:
            self.num_baits -= 1
            self.cache.pop(k)

    def do_lirs_eviction(self, ts):
        self.remove_baits_lru_end()
        evicted = self.cache.popitem(last=False)

        # the bottom of the stack can not be a bait.
        assert evicted[1].isNotBait(), "{}->{}".format(evicted[0], evicted[1])

        self.evictions += 1
        self.num_vals -= 1
        self.eviction_age_cum += ts - evicted[1].last_access_time
        if evicted[1].hits == 0:
            self.un_accessed_evictions += 1
            self.un_accessed_eviction_age_cum += ts - evicted[1].last_access_time

    def computeEvictionAge(self):
        if self.evictions == 0:
            return 0
        return round(self.eviction_age_cum / self.evictions, 2)

    def computeNoHitEvictionAge(self):
        if self.un_accessed_evictions == 0:
            return 0
        return round(self.un_accessed_eviction_age_cum / self.un_accessed_evictions, 2)


class QueueItem(object):
    def __init__(self, ts):
        self.last_access_time = ts
        self.hits = 0

    def markAccessed(self, ts):
        self.last_access_time = ts
        self.hits += 1


class QueueCache(object):
    def __init__(self, lru, num_elems, ap, filter_count, access_history_use_counts):
        self.lru = lru
        self.cache_size = num_elems
        self.cache = OrderedDict()
        self.ap = ap
        self.keys_written = 0
        self.rejections = 0
        self.evictions = 0
        self.eviction_age_cum = 0
        self.un_accessed_evictions = 0
        self.un_accessed_eviction_age_cum = 0
        # dynamic features, which have to be update on each request
        self.dynamic_features = dyn_features.DynamicFeatures(
            filter_count, access_history_use_counts
        )
        # queue for batch admissions
        self.admit_buffer = {}

    def str(self):
        return "size={}".format(len(self.cache))

    def find(self, key, key_ts):
        found = key in self.cache
        reuse_dist = 0
        if found:
            reuse_dist = key_ts - self.cache[key].last_access_time
            assert reuse_dist >= 0, "{} {}".format(key_ts, self.cache[key])
            self.cache[key].markAccessed(key_ts)

            # promote by removing and reinserting at the head.
            if self.lru:
                self.cache.move_to_end(key)
        # check if object in admission buffer --> hit
        if key in self.admit_buffer:
            found = True
        return found

    def insert(self, key, ts, keyfeaturelist):
        # record features
        self.admit_buffer[key] = self.dynamic_features.getFeature(key)

        self.admit_buffer[key].extend(keyfeaturelist)
        if len(self.admit_buffer) < ADMIT_BUFFER_LIMIT:
            # still space
            return
        # process batch admission
        decisions = self.ap.batchAccept(self.admit_buffer, ts)
        for nkey, dec in decisions.items():
            if not dec:
                self.rejections += 1
            else:
                # admit into cache
                self.cache[nkey] = QueueItem(ts)
                self.keys_written += 1
                if len(self.cache) >= self.cache_size:
                    self.do_eviction(ts)
                assert len(self.cache) < self.cache_size
        # empty buffer
        self.admit_buffer.clear()

    def do_eviction(self, ts):
        # pop in FIFO order
        evicted = self.cache.popitem(last=False)
        self.evictions += 1
        self.eviction_age_cum += ts - evicted[1].last_access_time
        if evicted[1].hits == 0:
            self.un_accessed_evictions += 1
            self.un_accessed_eviction_age_cum += ts - evicted[1].last_access_time

    def computeEvictionAge(self):
        if self.evictions == 0:
            return 0
        return round(self.eviction_age_cum / self.evictions, 2)

    def computeNoHitEvictionAge(self):
        if self.un_accessed_evictions == 0:
            return 0
        return round(self.un_accessed_eviction_age_cum / self.un_accessed_evictions, 2)


class AcceptAll(object):
    def __init__(self):
        None

    def accept(self, k, ts):
        return True

    def batchAccept(self, batch, ts):
        dec = {}
        for key in batch:
            dec[key] = True
        return dec


class RejectXAP(object):
    def __init__(self, threshold, window_count):
        self.window_count = window_count
        self.history = OrderedDict()
        self.threshold = threshold

    def accept(self, key, ts):
        # keep history only for window count
        if len(self.history) >= self.window_count:
            self.history.popitem(last=False)

        # if never seen before: reject
        if key not in self.history:
            self.history[key] = 1
            return False

        self.history[key] += 1
        if self.history[key] > self.threshold:
            return True
        return False

    def batchAccept(self, batch, ts):
        decisions = {}
        for key in batch:
            decisions[key] = self.accept(key, ts)
        return decisions


class CoinFlipAP(object):
    def __init__(self, probability):
        self.prob = probability

    def accept(self, kf, ts):
        if random.random() < self.prob:
            return True
        return False

    def batchAccept(self, batch, ts):
        decisions = {}
        for key in batch:
            decisions[key] = self.accept(None, ts)
        return decisions


# learned admission policy
class LearnedAP(object):
    def __init__(self, threshold, model_path=None, model_thrift=None):
        assert bool(model_path) != bool(model_thrift)
        self.threshold = threshold
        if model_path:
            self.gbm = lgb.Booster(model_file=model_path)
        else:
            self.gbm = lgb.Booster(
                model_str=model_thrift.parametersUnion.get_lightGBM().model_str
            )

    def batchAccept(self, batch, ts):
        features = np.array(list(batch.values()))
        # result:
        # dynF0 .. dynF5 kf1 kf2 kf3
        predictions = self.gbm.predict(features)
        decisions = dict(
            zip(batch.keys(), [pred > self.threshold for pred in predictions])
        )
        return decisions


# Rejects writes based on the amount of writes admitted so far. Computes the
# current write rate since begin and rejects based on the expected write rate.
# Assumes that the callee uses this as the leaf admission policy
class WriteRateRejectAP(object):
    def __init__(self, write_mbps, val_size):
        self.expected_rate = write_mbps
        self.bytes_written = 0
        self.start_ts = 0
        self.val_size = val_size

    def accept(self, k, ts):
        if self.expected_rate == 0:
            return True

        if self.start_ts == 0:
            self.start_ts = ts
        delta = float(ts - self.start_ts)
        assert delta >= 0
        if delta == 0:
            return False

        write_rate = (self.bytes_written + self.val_size) / delta
        if write_rate > self.expected_rate * 1024.0 * 1024.0:
            return False

        self.bytes_written += self.val_size
        return True

    def batchAccept(self, batch, ts):
        decisions = {}
        for key in batch:
            decisions[key] = self.accept(key, None, None, ts)
        return decisions


class RejectFirstWriteRateAP(object):
    def __init__(self, window_count, write_mbps, val_size):
        self.reject_first_ap = RejectXAP(1, window_count)
        self.write_rate_ap = WriteRateRejectAP(write_mbps, val_size)

    def accept(self, k, ts):
        return self.reject_first_ap.accept(k, ts) and self.write_rate_ap.accept(k, ts)

    def batchAccept(self, batch, ts):
        decisions = {}
        for key in batch:
            decisions[key] = self.accept(key, ts)
        return decisions


def simulate_cache(cache, accesses, sampling_ratio):
    # "stats" tracks intermediate results every 100000 iops.
    # This is mostly for debugging & analysis purposes since it allows us to
    # observe and compare how caching behaviors evolves over time
    stats_idx = 0
    stats = {
        "iops_requests": [0],  # number of total iops
        "iops_saved": [
            0
        ],  # number of iops saved (all chunks must be hit to be counted as iops save)
        "chunk_queries": [0],  # number of total chunk queries
        "chunk_hits": [0],  # chunk-level / CacheLib hit count
        "iops_partial_hits": [0],  # request with some chunks cached
        "chunks_written": [0],  # number of total chunks admitted to cache
        "duration": [0],  # duration in seconds
    }
    last_trace_time = 0
    last_chunks_written = 0
    write_mb_per_sec = 0
    print("duration(hours) | write_rate | iops_saved_ratio")
    for block_id, access in accesses:

        if last_trace_time == 0:
            last_trace_time = access.ts

        # stats management
        if stats["iops_requests"][stats_idx] > 100000:
            dur = access.ts - last_trace_time
            stats["duration"][stats_idx] = dur
            last_trace_time = access.ts
            stats["chunks_written"][stats_idx] = (
                cache.keys_written - last_chunks_written
            )
            last_chunks_written = cache.keys_written

            # logging intermediate results (fast-fail)
            iops_save_ratio = (
                stats["iops_saved"][stats_idx] / stats["iops_requests"][stats_idx]
            )
            write_mb_per_sec = utils.mb_per_sec(
                stats["chunks_written"][stats_idx], dur, sampling_ratio
            )
            print(dur / 3600, "|", write_mb_per_sec, "| ", iops_save_ratio)

            # seal stats and move idx
            for stat_key in stats.keys():
                stats[stat_key].append(0)
            stats_idx += 1

        stats["iops_requests"][stats_idx] += 1
        stats["chunk_queries"][stats_idx] += len(access.chunks())

        misses = []
        chunk_hit = False
        # iterate over each chunk of this request
        for chunk_id in access.chunks():
            k = (block_id, chunk_id)
            found = cache.find(k, access.ts)
            if found:
                stats["chunk_hits"][stats_idx] += 1
                chunk_hit = True
            else:
                misses.append(chunk_id)

        if len(misses) == 0:
            stats["iops_saved"][stats_idx] += 1
        else:
            for chunk_id in misses:
                k = (block_id, chunk_id)
                cache.insert(k, access.ts, access.features.toList())

            if chunk_hit:
                # request-level miss, but chunk_hit
                stats["iops_partial_hits"][stats_idx] += 1

        # update dynamic features (independent of in the cache)
        for chunk_id in access.chunks():
            k = (block_id, chunk_id)
            cache.dynamic_features.updateFeatures(k, access.ts)
    return stats


def simulate_cache_driver(options, args):
    if options.wsa_ap:
        assert len(args) == 3
        model_thrift = args[1]
        accesses = args[2]
    print(options)
    use_lru = not (options.fifo or options.lirs)
    assert use_lru or options.lirs or options.fifo

    tracefile = args[0]
    if not options.output_dir:
        output_dir = "/".join(tracefile.split("/")[:-1])
    else:
        output_dir = options.output_dir

    output_dir += utils.get_output_suffix(options)

    # create the output directory
    os.makedirs(output_dir, 0o755, exist_ok=True)

    input_file_name = tracefile[: -len(".trace")].split("/")[-1]
    out_file_name = "{}/{}_cache_perf.txt".format(output_dir, input_file_name)

    if options.wsa_ap:
        assert accesses
        trace_duration_secs = round(accesses[-1][1].ts - accesses[0][1].ts)
    else:
        accesses, start_ts, end_ts = utils.read_processed_file_list_accesses(
            tracefile, options.global_feature_map_path
        )
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
    logjson["options"] = options
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
            ap = RejectXAP(threshold, factor * num_cache_elems)
            apname = "RejectX"
        else:
            ap = RejectFirstWriteRateAP(
                factor * num_cache_elems, scaled_write_mbps, utils.BlkAccess.ALIGNMENT
            )
            apname = "RejectFirstWriteRate"
    elif options.learned_ap:
        assert options.learned_ap_model_path and options.ap_threshold
        ap = LearnedAP(options.ap_threshold, model_path=options.learned_ap_model_path)
        print(
            "Learned AP with model:",
            options.learned_ap_model_path,
            "threshold:",
            options.ap_threshold,
        )
        apname = "Learned-Regression"
    elif options.coinflip_ap:
        assert options.ap_probability
        ap = CoinFlipAP(options.ap_probability)
        print("CoinFlip AP with probability:", options.ap_probability)
        apname = "CoinFlip-P"
    elif options.wsa_ap:
        ap = LearnedAP(options.ap_threshold, model_thrift=model_thrift)
        print(
            "WSA AP with model:",
            options.model_name,
            "threshold:",
            options.ap_threshold,
        )
        apname = "LearnedAP-Regression-WSA"

    else:
        if scaled_write_mbps != 0:
            ap = WriteRateRejectAP(scaled_write_mbps, utils.BlkAccess.ALIGNMENT)
            apname = "WriteRateReject"
        else:
            ap = AcceptAll()
            apname = "AcceptAll"

    logjson["AdmissionPolicy"] = apname

    if options.lirs:
        cache = LIRSCache(
            num_cache_elems,
            1.0,
            ap,
            options.learned_ap_filter_count,
            options.access_history_use_counts,
        )
    else:
        cache = QueueCache(
            use_lru,
            num_cache_elems,
            ap,
            options.learned_ap_filter_count,
            options.access_history_use_counts,
        )

    stats = simulate_cache(cache, accesses, sampling_ratio)

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
        logjson["options"] = str(logjson["options"])
        json.dump(logjson, out)
    return logjson

#!/usr/bin/env python3

import datetime
import json
import os
from enum import Enum, unique

import numpy as np
import pandas as pd


@unique
class OpType(Enum):
    GET_TEMP = 1
    GET_PERM = 2
    PUT_TEMP = 3
    PUT_PERM = 4
    GET_NOT_INIT = 5
    UNKNOWN = 100


# key for various information in the raw trace file
KEY_STR = "BLOCKID"
OFF_STR = "OFFSET"
IOSIZE_STR = "IOSIZE"
OP_STR = "op"
PIPELINE_STR = "pipelineName"
NS_STR = "userNamespace"
USER_STR = "userName"

PUT_OPS = [OpType.PUT_PERM, OpType.PUT_TEMP]
# Used for unknown feature values to be consistent with
# prod config https://fburl.com/diffusion/clfwewx7
MAX_INT = 4294967295

# MODEL CONFIG
ACCESS_HISTORY_COUNT = 6
FEATURES = [f"bf_{i}" for i in range(0, ACCESS_HISTORY_COUNT)] + [
    "op",
    "namespace",
    "user",
]


class KeyFeatures(object):
    # helps reduce memory footprint
    __slots__ = ["op", "pipeline", "namespace", "user"]

    OP_TYPE_MAP = {
        "getChunkData.Temp": OpType.GET_TEMP,
        "getChunkData.Permanent": OpType.GET_PERM,
        "getChunkData.NotInitialized": OpType.GET_NOT_INIT,
        "putChunk.Permanent": OpType.PUT_PERM,
        "putChunk.Temp": OpType.PUT_TEMP,
    }

    def __init__(self, op, pipeline, namespace, user):
        self.op = OpType(op)
        self.pipeline = pipeline
        self.namespace = namespace
        self.user = user

    def __str__(self):
        return "op={}, pipeline={}, namespace={}, user={}".format(
            self.op, self.pipeline, self.namespace, self.user
        )

    def __repr__(self):
        # for easy debugging purpose
        return self.__str__()

    def toList(self):
        """Used in training and simulation for ML admission policies

        `pipeline` feature is not used in ML model due to it has too many categories to
        be encoded for production inference, as well as its collinearity with
        namespace and user, therefore omitted here.
        """
        return [self.op.value, self.namespace, self.user]


class BlkAccess(object):
    # chunk alignment for warm storage
    ALIGNMENT = 128 * 1024
    MAX_BLOCK_SIZE = 8 * 1024 * 1024

    # helps reduce memory footprint
    __slots__ = ["ts", "offset", "endoffset", "c", "features"]

    @staticmethod
    def roundDownToBlockBegin(off):
        return (off // BlkAccess.ALIGNMENT) * BlkAccess.ALIGNMENT

    @staticmethod
    def roundUpToBlockEnd(off):
        return (off // BlkAccess.ALIGNMENT + 1) * BlkAccess.ALIGNMENT - 1

    # offsets can be in the middle of a block. round them to the alignment to
    # emulate caching at a chunk level
    def __init__(self, offset, size, time, features=None):
        self.ts = time
        self.offset = BlkAccess.roundDownToBlockBegin(offset)
        self.endoffset = BlkAccess.roundUpToBlockEnd(offset + size - 1)
        # list of chunks
        self.c = []
        self.features = features

    def __str__(self):
        return "offset={}, size={}, ts={}, features={}".format(
            self.offset, self.size(), self.ts, self.features
        )

    def __repr__(self):
        # for easy debugging purpose
        return self.__str__()

    def size(self):
        return self.end() - self.start() + 1

    def start(self):
        return int(self.offset)

    def end(self):
        return int(self.endoffset)

    def chunks(self):
        if len(self.c) > 0:
            return self.c
        i = self.start()
        while i < self.end():
            i += BlkAccess.ALIGNMENT
            self.c.append(i // BlkAccess.ALIGNMENT)
        return self.c


class KeyAndAccesses(object):
    # helps reduce memory footprint
    __slots__ = ["key", "accesses"]

    def __init__(self, key):
        self.key = key
        self.accesses = []

    def addAccess(self, access):
        self.accesses.append(access)

    def sortAccesses(self):
        self.accesses.sort(key=lambda a: a.ts, reverse=False)


def process_line(users, namespaces, pipelines, kvs, timestamp):
    size = kvs[IOSIZE_STR]
    f = extract_features(users, namespaces, pipelines, kvs)

    offset = 0
    if f and f.op not in PUT_OPS:
        offset = kvs[OFF_STR]

    return BlkAccess(int(offset), int(size), float(timestamp), f)


# lookup k in m. If not present, generate a uuid and add it to map, return
# uuid
def lookup_or_add_uuid(m, k):
    if k not in m:
        v = len(m)
        m[k] = v
    return int(m[k])


def extract_features(users, namespaces, pipelines, kvs):
    def get_or_not_set(kvs, key):
        return kvs[key].strip("\n") if key in kvs else "NOT_SET"

    str_op = get_or_not_set(kvs, OP_STR)
    str_pipeline = get_or_not_set(kvs, PIPELINE_STR)
    str_user = get_or_not_set(kvs, USER_STR)
    str_namespace = get_or_not_set(kvs, NS_STR)

    op = None
    if str_op in KeyFeatures.OP_TYPE_MAP:
        op = KeyFeatures.OP_TYPE_MAP[str_op].value
    else:
        op = OpType.UNKNOWN.value

    pipeline = lookup_or_add_uuid(pipelines, str_pipeline)
    namespace = lookup_or_add_uuid(namespaces, str_namespace)
    user = lookup_or_add_uuid(users, str_user)
    return KeyFeatures(op, pipeline, namespace, user)


def process_line_and_add(accesses, keys, users, namespaces, pipelines, l, sample_rate):
    # Format is the following:
    # V0813 00:05:45.956594 BLOCKID=<blkid> OFFSET=<offset> IOSIZE=<size> ...
    # we assume a dummy year just to parse and generate time points at seconds
    # granularity in unix timestamps. The only down side is if we have a trace
    # file that spans across year boundary. In such a scenario, the trace
    # duration would be off by a big magnitude.

    try:
        parts = l.split(" ")
        formatted_time = "2020" + parts[0][1:] + parts[1]
        timestamp = datetime.datetime.strptime(
            formatted_time, "%Y%m%d%H:%M:%S.%f"
        ).timestamp()

        kvs = {}
        for p in parts[2:]:
            ps = p.split("=")
            if len(ps) == 2:
                kvs[ps[0]] = ps[1]

        # get the key
        k = kvs[KEY_STR]

        if not k:
            return

        if sample_rate < 100 and hash(k) % 100 > sample_rate:
            return

        ukey = lookup_or_add_uuid(keys, k)
        # first time we are seeing this
        if ukey not in accesses:
            accesses[ukey] = KeyAndAccesses(ukey)

        a = process_line(users, namespaces, pipelines, kvs, timestamp)
        accesses[ukey].addAccess(a)
    except (IndexError, ValueError, KeyError) as e:
        print("Error in line: ", parts, e)
        return


def read_tracefile(f, sample_rate):
    print("Reading trace file ", f)
    count = 0
    accesses = {}
    users = {}  # encoding of users to ints
    namespaces = {}  # encoding of namespaces to ints
    pipelines = {}  # encoding of pipelines to ints
    with open(f, "r") as of:
        keys = {}
        for l in of:
            count += 1
            process_line_and_add(
                accesses, keys, users, namespaces, pipelines, l, sample_rate
            )

    for k in accesses:
        accesses[k].sortAccesses()

    print("Read {} accesses and found {} keys".format(count, len(accesses)))
    return accesses, users, namespaces, pipelines


def write_access_to_file(f, a):
    print("Writing to {} records to  {}".format(len(a.items()), f))
    with open(f, "w") as of:
        for _, v in a.items():
            for a in v.accesses:
                line = "{} {} {} {}".format(v.key, a.offset, a.size(), a.ts)
                if a.features is not None:
                    line += " {} {} {} {}".format(
                        a.features.op.value,
                        a.features.pipeline,
                        a.features.namespace,
                        a.features.user,
                    )
                print(line, file=of)


def write_feature_encoding_to_file(f, m):
    print("Writing features to {} ".format(f))
    with open(f, "w") as of:
        for k, v in m.items():
            print("{} {}".format(k, v), file=of)


# read the processed file and return a dictionary of key to all its BlkAccess
# sorted by access time.
def read_processed_file(f, global_feature_map_path=None):
    """Read processd files and generate accesses

    Parameters:
        f : str
            processed trace path
        global_feature_map_path : str
            if provided, will perform feature mapping between local sampled trace and
            globel (cross-cluster) traces; This can be skipped when focussing on single
            cluster training/inference，but is critical to delopyment of production
            models to multiple clusters.
    """
    local_map_path = os.path.dirname(f)
    sample_ratio = int(os.path.basename(f).split("_")[-1].split(".")[0])

    if global_feature_map_path:
        (local_maps, global_maps) = get_feature_maps(
            local_map_path, global_feature_map_path, sample_ratio
        )
    print("Reading from file ", f)
    accesses = {}
    start_ts = None
    end_ts = None
    with open(f, "r") as of:
        key_map = {}
        for l in of:
            try:
                parts = l.split(" ")
                parts = [p.strip("\n") for p in parts]
                k = lookup_or_add_uuid(key_map, parts[0])
                off = int(parts[1])
                size = int(parts[2])
                ts = float(parts[3])

                f = None
                if len(parts) >= 8:
                    op = int(parts[4])

                    # if global_feature_map_path is provided, we map from local sampled
                    # feature index to global index provided to production model
                    if global_feature_map_path:
                        val = local_maps["namespace"][int(parts[6])]
                        namespace = (
                            global_maps["namespace"][val]
                            if val in global_maps["namespace"]
                            else MAX_INT
                        )

                        val = local_maps["user"][int(parts[7])]
                        user = (
                            global_maps["user"][val]
                            if val in global_maps["user"]
                            else MAX_INT
                        )
                    else:
                        namespace = int(parts[6])
                        user = int(parts[7])

                    # pipeline is not used by prod model, therefore no need to transform
                    pipeline = int(parts[5])

                    f = KeyFeatures(op, pipeline, namespace, user)

                # compute the time window of the trace
                start_ts = ts if start_ts is None else min(start_ts, ts)
                end_ts = ts if end_ts is None else max(end_ts, ts)

                if k not in accesses:
                    accesses[k] = KeyAndAccesses(k)
                accesses[k].addAccess(BlkAccess(off, size, ts, f))
            except (ValueError, IndexError):
                print("Error in parsing line ", l, parts)

    for k in accesses:
        accesses[k].sortAccesses()

    return accesses, start_ts, end_ts


# read the processed file and return  list of (k, BlkAccess) sorted by access time.
def read_processed_file_list_accesses(f, global_feature_map_path=None):
    """Read processd files and generate accesses sorted by time

    Parameters:
        f : str
            processed trace path
        global_feature_map_path : str
            if provided, will perform feature mapping between local sampled trace and
            globel (cross-cluster) trace; This is critical to delopy production models,
            but can be skipped for research purposes
    """
    k_accesses, start_ts, end_ts = read_processed_file(f, global_feature_map_path)
    accesses = []

    for k in k_accesses:
        for a in k_accesses[k].accesses:
            accesses.append((k, a))
    accesses.sort(key=lambda a: float(a[1].ts), reverse=False)

    return accesses, start_ts, end_ts


def make_format_string(fields):
    print_fmt_hdr = "{0[0]:<12}"
    print_fmt_line = "{:<12}"

    for i in range(len(fields) - 1):
        print_fmt_hdr += " {0[" + str(i + 1) + "]:<20}"
        print_fmt_line += " {:<20}"
    return print_fmt_hdr, print_fmt_line


def get_output_suffix(options):
    out = "/"
    # admission policy notes
    if options.rejectx_ap:
        out += (
            "rejectx-ap-"
            + str(options.ap_threshold)
            + "_"
            + str(options.ap_probability)
        )
    elif options.learned_ap:
        out += (
            "ml-ap-"
            + str(options.ap_threshold)
            + "_"
            + str(options.learned_ap_filter_count)
        )
    elif options.coinflip_ap:
        out += "coinflip-ap-" + str(options.ap_probability)

    # cache type notes
    if options.lirs:
        out += "_lirs"
    elif options.fifo:
        out += "_fifo"
    else:
        out += "_lru"

    if options.write_mbps != 0:
        out += "-{}".format(options.write_mbps)

    out += "_{}GB".format(options.size_gb)
    return out


def pct(x, y):
    return round(x * 100.0 / y, 2)


def mb_per_sec(chunks, time_secs, sampling_ratio):
    return round(
        (chunks * BlkAccess.ALIGNMENT * 100.0)
        / (time_secs * 1024 * 1024 * sampling_ratio),
        2,
    )


def find_nearest(array, value):
    """get index of the value in a list that are closest to target value

    Used for selecting probability thresholds given target recall
    """
    array = np.asarray(array)
    idx = (np.abs(array - value)).argmin()
    return idx


def get_feature_maps(local_map_path, global_map_path, sample_ratio):
    """load local feature map and global feature map for prod model

    Local map is generated when sampling trace in a single cluster,
    e.g. `processed_all_features_1.0.users`;
    Global map is the feature mapping maintained for prod model;

    To ensure the prod model translates incoming feature strings to
    the correct internal integter representation,
    transformation is essential before training.
    """
    local_maps = {}
    for feature in ["namespace", "user"]:
        df = pd.read_csv(
            f"{local_map_path}/processed_all_features_{sample_ratio}.0.{feature}s",
            sep=" ",
            header=None,
        )
        local_maps[feature] = dict(zip(df[1], df[0]))
    ## load global string to index feature mappings
    with open(global_map_path) as f:
        global_maps = json.load(f)
    return (local_maps, global_maps)

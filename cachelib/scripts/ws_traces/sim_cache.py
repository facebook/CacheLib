from __future__ import absolute_import, division, print_function, unicode_literals

from collections import OrderedDict


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
    def __init__(self, num_elems, bait_factor, ap):
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

        return (found and not is_bait), reuse_dist

    def insert(self, key, ts):
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
    def __init__(self, lru, num_elems, ap):
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
        return found, reuse_dist

    def insert(self, key, ts):
        if not self.ap.accept(key, ts):
            self.rejections += 1
            return

        if len(self.cache) >= self.cache_size:
            self.do_eviction(ts)

        assert len(self.cache) < self.cache_size

        self.cache[key] = QueueItem(ts)
        self.keys_written += 1

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


class RejectFirstAP(object):
    def __init__(self, window_count):
        self.window_count = window_count
        self.history = OrderedDict()

    def accept(self, k, ts):
        if k in self.history:
            return True

        if len(self.history) >= self.window_count:
            self.history.popitem(last=False)

        self.history[k] = None
        return False


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


class RejectFirstWriteRateAP(object):
    def __init__(self, window_count, write_mbps, val_size):
        self.reject_first_ap = RejectFirstAP(window_count)
        self.write_rate_ap = WriteRateRejectAP(write_mbps, val_size)

    def accept(self, k, ts):
        return self.reject_first_ap.accept(k, ts) and self.write_rate_ap.accept(k, ts)

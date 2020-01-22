from __future__ import absolute_import, division, print_function, unicode_literals


class CacheStats(object):
    # helps reduce memory footprint
    __slots__ = [
        "misses",
        "reuse_dist",
        "chunks_written",
        "prevs",
        "chunks_accessed",
        "chunks_hit",
        "reject_first",
        "fifo",
    ]

    # by default simulate a perfect lru
    def __init__(self, reject_first=False, fifo=False):
        self.misses = 0
        self.reuse_dist = []
        self.chunks_written = 0
        # list of previous chunks accessed
        self.prevs = {}
        self.chunks_accessed = 0
        self.chunks_hit = 0
        self.reject_first = reject_first
        self.fifo = fifo

    def markHit(self, reuse_dist):
        self.reuse_dist.append(reuse_dist)

    def markMiss(self):
        self.misses += 1

    def totalAccess(self):
        return self.numHits() + self.numMisses()

    def numHits(self):
        return len(self.reuse_dist)

    def numMisses(self):
        return self.misses

    def numChunksAccessed(self):
        return self.chunks_accessed

    def numChunksHit(self):
        return self.chunks_hit

    def numChunksWritten(self):
        return self.chunks_written

    def isSingleAccess(self):
        # there was just one access
        return self.numMisses() == 1 and self.numHits() == 0

    def isNonCacheable(self):
        # none of them were hits.
        return self.numHits() == 0

    def isFullCacheable(self):
        # we have to tolerate the first access that results in a miss.
        return self.numHits() > 0 and self.numMisses() == 1

    @staticmethod
    def reuse_distance(x, now):
        assert float(now) >= float(x), "{} vs {}".format(x, now)
        return float(now) - float(x)

    # true if x's reuse is wihtin the cache lifetime
    @staticmethod
    def in_cache(x, now, cache_lifetime):
        return (
            cache_lifetime == 0 or CacheStats.reuse_distance(x, now) <= cache_lifetime
        )

    def insert_chunks(self, chunks, ts):
        for c in chunks:
            self.insert_chunk(c, ts)

    def insert_chunk(self, c, ts):
        if self.reject_first:
            # if an older timestamp exists and we are inserting, it means it
            # actually is out of cache. so treat it as not existing
            should_reject = c not in self.prevs or self.prevs[c] != 0
            if should_reject:
                self.prevs[c] = 0  # mark as seen the first time
                return

        self.prevs[c] = ts
        self.chunks_written += 1

    def chunk_in_cache(self, c, ts, cache_lifetime):
        return c in self.prevs and CacheStats.in_cache(
            self.prevs[c], ts, cache_lifetime
        )

    # based on the cache lifetime, compute if the previous accesses contains the
    # same accessed blocks as current access. Returns the reuse distance and
    # whether it was a cache hit.
    def sim_cache_access(self, curr, cache_lifetime):
        if len(self.prevs) == 0:
            self.insert_chunks(curr.chunks(), curr.ts)
            self.markMiss()
            return

        miss = False
        reuse_dist = None
        for c in curr.chunks():
            if self.chunk_in_cache(c, curr.ts, cache_lifetime):
                d = CacheStats.reuse_distance(self.prevs[c], curr.ts)
                reuse_dist = d if reuse_dist is None else max(reuse_dist, d)
                # update the timstamp only if we are not emulating a fifo and
                # doing an LRU
                if not self.fifo:
                    self.prevs[c] = curr.ts
                self.chunks_hit += 1
            else:
                self.insert_chunk(c, curr.ts)
                miss = True

        if miss:
            self.markMiss()
        else:
            self.markHit(reuse_dist)

        return

    def sim_cache(self, accesses, cache_lifetime=0):
        for curr in accesses:
            self.chunks_accessed += len(curr.chunks())
            self.sim_cache_access(curr, cache_lifetime)
        return

#!/usr/bin/env python3
import logging
import random

import numpy as np


logger = logging.getLogger(__name__)


LOGGING_COUNT = 100000


class FeatureExtractor:
    """
    Randomly accept items into cache and record how many times it will be accessed
    in future flashEvictionAge (seconds)
    Build feature at accpetance - past access history, static features

    Update "bloomfilters" on every access to track full history
    """

    def __init__(self, flashEvictionAge, samplingProb, dfeature):
        self.featureGather = {}
        # dict from key (blk_id, chunk_id, hostname) to a list of featureLabels.
        # A featureLabel contains the feature from data, past accesses and the label (future accesses)
        self.labelGather = {}
        # Sampling probability for chunks.
        self.samplingProb = samplingProb
        # Eviction age in seconds that defines the future accesses' interval
        self.flashEvictionAge = flashEvictionAge
        # Latest timestamp of the processed traces.
        self.latestTimestamp = 0
        # Access histories
        self.dynamicFeatures = dfeature

        # To efficiently update the label, we define a sliding window on each key's list of featureLabels.
        # This window tracks the data points for the last lookahead interval defined by flashEvictionAge.
        # The "label" field of the featureLabels in this time window is the number of accesses between the current sample and the next picked sample.
        # The "label" field of the featureLabels outside this time window is the number of future accesses in the entire lookahead window.
        # The sliding window for key k starts at self.labelGather[k][self.slidingWindowIdx[k]] and ends at the end of self.labelGather[k]
        self.slidingWindowIdx = {}
        # The number of accesses in the sliding window for a certain key.
        self.accumulatedAccesses = {}

        # debugging information for how many hosts per block_id spans.
        self.blkId_hosts = {}

    def run(self, accesses):
        """
        Main function for FeatureExtractor.

        Process each accessed chunks chronologically, this includes:
        1. update keyFeature,
        2. update existing label(access counts),
        3. add current accessed chunk to training data with probability "samplingProb"
        4. update cache access history (bloomfilter)

        Log processing progress every 100000 accesses
        """
        for idx, (blkId, access) in enumerate(accesses):
            # iterate over each chunk of this request
            for chunkId in access.chunks():
                key = (blkId, chunkId, access.hostname)
                if blkId not in self.blkId_hosts.keys():
                    self.blkId_hosts[blkId] = {access.hostname}
                else:
                    self.blkId_hosts[blkId].add(access.hostname)
                self.processRequest(key, access.ts, access.features)
            # track progress
            if (idx + 1) % LOGGING_COUNT == 0:
                progress = round(idx / len(accesses), 4) * 100
                logger.info(f"#Access: {idx+1}. {progress} %")

        logger.info("# Closing out sliding windows")
        # Close out the sliding windows
        for key in self.labelGather.keys():
            while self.slidingWindowIdx[key] < len(self.labelGather[key]):
                self._slideWindow(key)

        hosts = [len(self.blkId_hosts[k]) for k in self.blkId_hosts.keys()]
        hosts = np.array(hosts)
        logger.info("avg host: {}".format(np.mean(hosts)))
        logger.info("min: {}".format(np.min(hosts)))
        logger.info("max: {}".format(np.max(hosts)))
        logger.info("std: {}".format(np.std(hosts)))

    def processRequest(self, key, ts, keyFeatures):
        # update internal timestamp
        if ts > self.latestTimestamp:
            self.latestTimestamp = ts
        # add key and its static features (key features keyFeatures) to feature map
        if key not in self.featureGather:
            self.featureGather[key] = keyFeatures

        # update labels
        if key in self.labelGather:
            featureList = self.labelGather[key]

            # Slide the window
            while (
                self.slidingWindowIdx[key] < len(featureList)
                and ts - featureList[self.slidingWindowIdx[key]]["samplingTime"]
                >= self.flashEvictionAge
            ):
                self._slideWindow(key)

            if ts - featureList[-1]["samplingTime"] < self.flashEvictionAge:
                self.labelGather[key][-1]["label"] += 1
                self.accumulatedAccesses[key] += 1

        # randlomly add new access to dataset
        if random.random() < self.samplingProb:
            newFeatureLabel = {
                "label": 0,  # number of occurrances in the future eviction_age interval
                "samplingTime": ts,  # the time this sample is generated
                "keyFeatures": keyFeatures,  # the features
                "bloomfilters": self.dynamicFeatures.getFeature(key),
            }
            if key in self.labelGather:
                self.labelGather[key].append(newFeatureLabel)
            else:
                self.labelGather[key] = [newFeatureLabel]
                self.slidingWindowIdx[key] = 0
                self.accumulatedAccesses[key] = 0

        # update dynamic features for each access, this is independent of sampling
        self.dynamicFeatures.updateFeatures(key, ts)

    def createFeatureTable(self):
        # aggregate into list of lists
        features = []
        labels = []
        for _, featureList in self.labelGather.items():
            for featureLabel in featureList:
                # only export training data sampled longer than one hour (3600 s)ago
                if featureLabel["samplingTime"] + 3600 < self.latestTimestamp:
                    # each row consists of label,dynamiceFeatures,keyFeatures
                    labels.append(featureLabel["label"])
                    featuretupel = featureLabel["bloomfilters"].copy()
                    featuretupel.extend(featureLabel["keyFeatures"].toList())
                    features.append(featuretupel)
        return (np.array(labels), np.asarray(features))

    def _slideWindow(self, key):
        self.accumulatedAccesses[key] -= self.labelGather[key][
            self.slidingWindowIdx[key]
        ]["label"]
        self.labelGather[key][self.slidingWindowIdx[key]][
            "label"
        ] += self.accumulatedAccesses[key]
        self.slidingWindowIdx[key] += 1

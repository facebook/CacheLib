#!/usr/bin/env python3
import random

import numpy as np


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
        self.labelGather = {}
        self.samplingProb = samplingProb
        self.flashEvictionAge = flashEvictionAge
        self.latestTimestamp = 0
        self.dynamicFeatures = dfeature

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
                key = (blkId, chunkId)
                self.processRequest(key, access.ts, access.features)
            # track progress
            if (idx + 1) % LOGGING_COUNT == 0:
                progress = round(idx / len(accesses), 4) * 100
                print(f"#Access: {idx+1}. {progress} %")

    def processRequest(self, key, ts, keyFeatures):
        # update internal timestamp
        if ts > self.latestTimestamp:
            self.latestTimestamp = ts
        # add key and its static features (key features keyFeatures) to feature map
        if key not in self.featureGather:
            self.featureGather[key] = keyFeatures

        # update labels
        if key in self.labelGather:
            # update previous access
            featureList = self.labelGather[key]
            for idx, feature in enumerate(featureList):
                if (ts - feature["samplingTime"]) < self.flashEvictionAge:
                    # within flash eviction age
                    self.labelGather[key][idx]["label"] += 1

        # randlomly add new access to dataset
        if random.random() < self.samplingProb:
            newFeatureLabel = {
                "label": 0,
                "samplingTime": ts,
                "keyFeatures": keyFeatures,
                "bloomfilters": self.dynamicFeatures.getFeature(key),
            }
            if key in self.labelGather:
                self.labelGather[key].append(newFeatureLabel)
            else:
                self.labelGather[key] = [newFeatureLabel]

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

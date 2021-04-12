#!/usr/bin/env python3

import logging
import random
from datetime import timedelta

import cachelib.scripts.ws_traces.feature_extractor as feature_extractor
import cachelib.scripts.ws_traces.utils as utils
import fblearner.flow.projects.cachelib.unified.common.dynamic_features as dfeature
import numpy as np
import pandas as pd
from fblearner.flow.projects.cachelib.unified.common.historical_access import (
    create_historical_accesses,
)
from fblearner.flow.projects.cachelib.unified.common.labelizer import create_labels


logger = logging.getLogger(__name__)


# Max number of rows used for training & testing
MAX_ROWS = int(5 * 1e6)
# Percentage of chunks to be tracked for generating training data
CHUNK_SAMPLE_RATIO = 0.02
# [LabelConfig] admit (label = 1) if past number of accesses >= LABEL_REJECTX
LABEL_REJECTX = 1
# [LabelConfig] admit (label = 1) if
# future number of accesses > LABEL_FUTURE_ACCESS_THRESHOLD
LABEL_FUTURE_ACCESS_THRESHOLD = 2


def build_dataset_from_accesses(
    eviction_age: int, model_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Builds training dataset from acccesses trace
    """

    logger.info("#### Start building dataset")
    # WSML uses a composite key.
    # e.g. block 1 chunk 1 on host A is different from block 1 chunk 1 on host B
    logger.info("Computing key")
    model_df["key"] = (
        model_df["block_id"]
        + "|"
        + model_df["chunk"].astype(str)
        + "|"
        + model_df["host_name"]
    )

    # Compute label
    logger.info("Creating labels")
    model_df["label"] = create_labels(
        model_df["key"],
        model_df["op_time"],
        ts_delta_start=timedelta(),
        ts_delta_end=timedelta(seconds=eviction_age),
    )

    # Featurize
    logger.info("Computing historical accesses")
    num_buckets = 6
    historical_accesses = create_historical_accesses(
        model_df["key"],
        model_df["op_time"],
        timedelta(),
        timedelta(hours=1),
        num_buckets,
    )
    logger.info("Assigning AH columns")
    model_df = model_df.assign(
        **{f"bf_{i}": history for i, history in enumerate(historical_accesses)}
    )
    logger.info("Computing 'missSize'")
    model_df["missSize"] = np.sum(
        model_df[[f"bf_{i}" for i in range(0, utils.ACCESS_HISTORY_COUNT)]], axis=1
    )

    # Rename columns to prod format
    model_df["namespace"] = model_df["user_namespace"]
    model_df["user"] = model_df["user_name"]

    # Map op_name to prod int mapping
    op_map = pd.CategoricalDtype(
        [
            "getChunkData.Temp",
            "getChunkData.Permanent",
            "putChunk.Temp",
            "putChunk.Permanent",
            "getChunkData.NotInitialized",
        ]
    )
    model_df["op"] = model_df["op_name"].astype(op_map).cat.codes
    model_df["op"].loc[
        model_df["op"] != -1
    ] += 1  # to match cachelib/scripts/ws_traces/utils.py

    # Drop extra cols
    cols_to_delete = [
        col
        for col in list(model_df.columns)
        if col not in utils.FEATURES + ["label", "missSize"]
    ]
    for col in cols_to_delete:
        del model_df[col]

    return model_df


def train_test_split(model_df):
    # split training validation set with ratio 4:1
    train_test_split = int(model_df.shape[0] * 0.8)

    X_train = model_df.loc[:train_test_split, :].drop(columns=["label", "missSize"])
    y_train = (
        model_df.loc[:train_test_split, "label"] > LABEL_FUTURE_ACCESS_THRESHOLD
    ) & (model_df.loc[:train_test_split, "missSize"] >= LABEL_REJECTX)

    X_test = model_df.loc[train_test_split:, :].drop(columns=["label", "missSize"])
    y_test = (
        model_df.loc[train_test_split:, "label"] > LABEL_FUTURE_ACCESS_THRESHOLD
    ) & (model_df.loc[train_test_split:, "missSize"] >= LABEL_REJECTX)

    return X_train, y_train, X_test, y_test

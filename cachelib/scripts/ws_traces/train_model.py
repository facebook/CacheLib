#!/usr/bin/env python3

import logging
import random

import cachelib.scripts.ws_traces.feature_extractor as feature_extractor
import cachelib.scripts.ws_traces.utils as utils
import fblearner.flow.projects.cachelib.unified.common.dynamic_features as dfeature

# @dep=@/third-party:lightgbm:lightgbm-py
import lightgbm as lgb  # @manual
import numpy as np
import pandas as pd


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


def build_dataset_for_training(
    tracefile,
    region,
    sample_ratio,
    eviction_age,
    access_history_use_counts=True,
    global_feature_map_path=None,
):
    accesses, start_ts, end_ts = utils.read_processed_file_list_accesses(
        tracefile, global_feature_map_path
    )
    return build_dataset_from_accesses(
        eviction_age, access_history_use_counts, accesses
    )


def build_dataset_from_accesses(eviction_age, access_history_use_counts, accesses):
    logger.info("#### Start building dataset")
    dynamicFeatures = dfeature.DynamicFeatures(
        utils.ACCESS_HISTORY_COUNT, access_history_use_counts
    )
    extractor = feature_extractor.FeatureExtractor(
        eviction_age, CHUNK_SAMPLE_RATIO, dynamicFeatures
    )
    logger.info("##### Start running feature extractor")
    extractor.run(accesses)
    logger.info("###### Finished running feature extractor")

    # create final model based on split train/test, which enables early stopping
    (labels, features) = extractor.createFeatureTable()

    randIndex = random.sample(range(len(labels)), min(len(labels), MAX_ROWS))
    model_df = pd.DataFrame(features[randIndex], columns=utils.FEATURES)

    model_df["label"] = labels[randIndex]

    model_df["missSize"] = np.sum(
        model_df[[f"bf_{i}" for i in range(0, utils.ACCESS_HISTORY_COUNT)]], axis=1
    )

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


def train_lgbm_model(X_train, y_train, X_test, y_test):

    # basic model training hyperparameters
    # hyperparameters are chosen based on experience & offline tuning
    # Model AUC is gernerally above 0.95 (good enough), therefore the performance
    # is more bounded by framing the right objective than training the model.

    lgb_train = lgb.Dataset(X_train, y_train)
    lgb_eval = lgb.Dataset(X_test, y_test, reference=lgb_train)

    params = {
        "boosting_type": "gbdt",
        "objective": "binary",
        "metric": ["binary_logloss"],
        "num_leaves": 63,
        "learning_rate": 0.005,
        "max_bin": 255,
        "feature_fraction": 0.9,
        "bagging_fraction": 0.9,
        "bagging_freq": 5,
        "min_data_in_leaf": 50,
        "min_sum_hessian_in_leaf": 5.0,
        "num_threads": 10,
        "verbosity": -1,
    }

    gbm = lgb.train(
        params,
        lgb_train,
        num_boost_round=500,
        valid_sets=lgb_eval,
        verbose_eval=False,
        early_stopping_rounds=25,
    )
    return gbm

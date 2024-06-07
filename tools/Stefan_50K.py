#!/usr/bin/env python3
""" 
A typical script for ML-based edge filtering.
ML standing for Matteo Lacki, for goodness sake...
"""
import argparse
import collections
import functools
import json
from math import inf
from pathlib import Path
from pprint import pprint

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tomllib
from dia_common.devtools import get_data_for_development
from midia_search_engines.models.models import BaseEdgeDetectionModel, show_or_save
from midia_search_engines.multiple_search_filters import (
    edge_positive_or_touching_a_not_yet_explained_fragment,
)

#### HELPERs
from midia_search_engines.precursor_fragment_graph import (
    get_and_preprocess_data,
    get_edge_stats,
)
from midia_search_engines.stats import get_fragment_ranks, plot_ranks
from pandas_ops.io import read_df, save_df
from pandas_ops.iteration import iter_df_batches
from pandas_ops.misc import add_column_to_pandas_dataframe_without_copying_data
from recapuccino.importing import dynamically_import_foo
from recapuccino.misc import in_ipython

if in_ipython():
    from IPython import get_ipython

    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")


if in_ipython():
    # dataset = "G8027"
    # calibration = "G8045"
    dataset = "G8602"
    calibration = "G8605"
    path_to_the_folder_with_data = Path(
        # f"outputs/edgeDepletion/{dataset}/{calibration}/{fillit}"
        f"outputs/edgeRefinement/{dataset}/{calibration}/baseEdgeStats^positiveEllipse^maxRank=12_depleted"
    )
    args = get_data_for_development(
        path_to_the_folder_with_data=path_to_the_folder_with_data,
        MS1_stats_path="ms1_cluster_stats.parquet",
        MS2_stats_path="ms2_cluster_stats.parquet",
        rough_edges="rough_edges.startrek",
        ml_edges="ml_edges.parquet",
        config="config.toml",
    )
    args.progressbar_message = "test"
    args.output = Path("/tmp/test_edges.startrek")
    args.stats_output_path = Path("/tmp/test_edges_stats.json")
    args.plots_folder = Path("/tmp/test_plots")
    args.width = 10
    args.height = 10
    args.dpi = 100
    args.style = "dark_background"
    args.cmap = "inferno"
    args.batch_size = 100_000_000
    args.verbose = True


assert not args.output.exists(), f"Output folder {args.output} already exists."
with open(args.config, "rb") as f:
    config = tomllib.load(f)
if args.verbose:
    pprint(config)


ml_edges = (
    read_df(args.ml_edges)
    .sort_values(["MS1_ClusterID", "MS2_ClusterID"])
    .reset_index(drop=True)
)

ml_edges_0 = ml_edges.query("Y==0")
ml_edges_1 = ml_edges.query("Y==1")

assert len(ml_edges_0) == len(ml_edges_1)
K = 25_000

drawn_edges = (
    pd.concat(
        [
            ml_edges_0.iloc[np.sort(np.random.randint(len(ml_edges_0), size=K))],
            ml_edges_1.iloc[np.sort(np.random.randint(len(ml_edges_1), size=K))],
        ],
        axis=0,
    )
    .sort_values(["MS1_ClusterID", "MS2_ClusterID"])
    .reset_index(drop=True)
)


sqls = dict(
    ms1_variables_to_substract="""
    SELECT
    mz_wmean AS precursor_mz_wmean,
    scan_wmean,
    retention_time_wmean
    FROM '{MS1_stats_path}'
    """,
    ms2_variables_to_substract="""
    SELECT
    precursor_mz_pred_fastmist AS precursor_mz_wmean,
    scan_wmean,
    retention_time_wmean
    FROM '{MS2_stats_path}'
    """,
    ms1_variables_to_take="""
    SELECT
    frame_max - frame_min AS frame_extent,
    retention_time_max - retention_time_min AS retention_time_extent,
    inv_ion_mobility_max - inv_ion_mobility_min AS inv_ion_mobility_extent,
    *
    FROM '{MS1_stats_path}'
    """,
    ms2_variables_to_take="""
    SELECT
    frame_max - frame_min AS frame_extent,
    retention_time_max - retention_time_min AS retention_time_extent,
    inv_ion_mobility_max - inv_ion_mobility_min AS inv_ion_mobility_extent,
    *,
    FROM '{MS2_stats_path}'
    """,
)


preprocessed_data = {
    name: get_and_preprocess_data(sql, **args.__dict__) for name, sql in sqls.items()
}

preprocessed_data = dict(
    zip(
        preprocessed_data,
        map(
            lambda d: d[
                d.columns[
                    list(map(lambda x: not pd.api.types.is_object_dtype(x), d.dtypes))
                ]
            ],
            preprocessed_data.values(),
        ),
    )
)

edge_stats = get_edge_stats(
    MS1_ClusterIDs=drawn_edges.MS1_ClusterID,
    MS2_ClusterIDs=drawn_edges.MS2_ClusterID,
    diff_columns=config["data"]["diff_columns"],
    **preprocessed_data,
)

assert np.all(edge_stats.ms1_ClusterID == drawn_edges.MS1_ClusterID)
assert np.all(edge_stats.ms2_ClusterID == drawn_edges.MS2_ClusterID)

edge_stats["is_positive"] = drawn_edges.Y
edge_stats.to_csv("/mnt/ms/new/processed/MIDIA/edges/G8602_cG8605_50K.csv")

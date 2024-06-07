#!/usr/bin/env python3
import argparse
from pathlib import Path
from pprint import pprint
from types import SimpleNamespace

from tqdm import tqdm

import duckdb
import numpy as np
import pandas as pd
import tomllib
from mmapped_df import DatasetWriter
from pandas_ops.io import read_df
from pandas_ops.iteration import iter_df_batches, iter_start_end_tuples
from pandas_ops.sortedness import is_sorted_lexicographically, is_strictly_increasing
from recapuccino.misc import in_ipython

if in_ipython():
    from IPython import get_ipython
    from subprocess import run

    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", 20)

    _folder = Path("out/base/debug_filter_rough_matches")
    run(["snakemake", "-call", _folder])

    args = dict(
        edges=_folder / "rough_matches.startrek",
        config=Path("configs/edge_filters/high_correlation.toml"),
        output=Path("/home/matteo/filtered_edges.startrek"),
        progressbar_message="chuj",
        silent=False,
    )

else:
    parser = argparse.ArgumentParser(
        description="Filter edges.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "edges",
        help="Path to edges in '.startrek' format.",
        type=Path,
    )
    parser.add_argument(
        "config",
        help="Config specifying the filtering crirteria.",
        type=Path,
    )
    parser.add_argument(
        "output",
        help="Where to store the edges in the `.startrek` format.",
        type=Path,
    )
    parser.add_argument(
        "--progressbar_message",
        help="What message should appear in hte progressbar.",
        default="Filtering edges.",
        type=str,
    )
    parser.add_argument(
        "--silent",
        help="Don't print to stdout.",
        action="store_true",
    )
    args = parser.parse_args().__dict__

if __name__ == "__main__":
    duckconn = duckdb.connect()

    with open(args["config"], "rb") as f:
        config = tomllib.load(f)
        if not args["silent"]:
            pprint(config)

    edges = read_df(args["edges"])
    if not args["silent"]:
        print(edges.head(2))
    edges["original_idx"] = np.arange(0, len(edges), dtype=np.uint64)

    assert is_sorted_lexicographically(
        True, edges.MS1_ClusterID, edges.MS2_ClusterID
    ), "Michał or Matteo (latter unlikely) did not sort data lexicographically first by MS1_ClusterID and then by MS2_ClusterID."

    out_edges = DatasetWriter(args["output"])

    _query = config["filter_sql"].format(local_edges="local_edges")
    with tqdm(total=len(edges), desc="Filtering Rows (filter_edges.py)") as pbar:
        # start_idx, end_idx = next(iter_start_end_tuples(config["batch_size"], len(edges)))
        for start_idx, end_idx in iter_start_end_tuples(
            config["batch_size"], len(edges)
        ):
            local_edges = edges.iloc[start_idx:end_idx]
            local_edges = duckconn.query(_query).df()
            out_edges.append_df(local_edges)
            pbar.update(end_idx - start_idx)

    output = read_df(args["output"])
    assert is_strictly_increasing(output.original_idx), "Table not sorted."

    # TODO: ADD THE MIN PEAKS FILTER BACK!

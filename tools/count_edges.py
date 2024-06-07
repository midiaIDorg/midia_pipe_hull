#!/usr/bin/env python3

import argparse
import json
from pathlib import Path

import pandas as pd
from pandas_ops.io import read_df
from pandas_ops.uniqueness import get_unique, get_unique_sorted
from recapuccino.misc import in_ipython

if False:
    from warnings import warn
    from IPython import get_ipython
    from subprocess import run

    warn("Development mode")
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
    pd.set_option("display.max_columns", None)

    _folder = Path("out/base/debug_count_edges")
    run(["snakemake", "-call", _folder])

    args = dict(
        edges=_folder / "rough_matches.startrek",
        out="/tmp/test.counts.csv",
    )
else:
    parser = argparse.ArgumentParser(
        description="Get numbers of distinct edges in a file."
    )
    parser.add_argument(
        "edges",
        help="Path to the .startrek or .parquet with some edges.",
        type=Path,
    )
    parser.add_argument(
        "out",
        help="Where to save the json with unique MS1_ClusterID and unique MS2_ClusterID and overall number of edges.",
        type=Path,
    )
    args = parser.parse_args().__dict__


if __name__ == "__main__":
    key = ["MS1_ClusterID", "MS2_ClusterID"]
    edges = read_df(args["edges"], columns=key)[key]
    counts = dict(
        MS1_ClusterID=len(get_unique_sorted(edges.MS1_ClusterID)),
        MS2_ClusterID=len(get_unique(edges.MS2_ClusterID)),
        edges=len(edges),
    )
    pd.DataFrame([counts]).to_csv(args["out"], index=False, index_label=False)

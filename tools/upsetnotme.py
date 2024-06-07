#!/usr/bin/env python3
""" 
Make upsetplots of any number of tables with aritrary configurable sqls.
"""
import argparse
from collections import Counter
from pathlib import Path

import upsetplot

import duckdb
import matplotlib.pyplot as plt
import numba
import numpy as np
import pandas as pd
import tomllib

if False:
    from IPython import get_ipython
    from subprocess import run

    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")

    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", 20)
    _folder = Path("out/base/dev_upsetplots")
    _out = Path("/tmp/upsets")
    _out.mkdir(exist_ok=True)

    # _folder = Path("out/base/debug_edge_refinement_old")
    run(["snakemake", "-call", _folder])
    args = dict(
        config=Path("configs/plots/upsets/peptide_counts.toml"),
        tables=[
            # _folder / "first_gen_results_sage.parquet",
            _folder / "first_gen_filtered_sage_precursors.parquet",
            # _folder / "second_gen_results_sage.parquet",
            _folder / "second_gen_filtered_sage_precursors.parquet",
            # _folder / "first_gen_matched_fragments_sage.parquet",
            # _folder / "second_gen_matched_fragments_sage.parquet",
            # _folder / "first_gen_filtered_sage_fragments.parquet",
            # _folder / "second_gen_filtered_sage_fragments.parquet",
        ],
        out=_out,
        silent=False,
    )
else:
    parser = argparse.ArgumentParser(
        description="Make some upset plots.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "config",
        help="Config containing sqls.",
        type=Path,
    )
    parser.add_argument(
        "tables",
        help="Paths to tables to compare.",
        nargs="+",
        type=Path,
    )
    parser.add_argument(
        "--out",
        help="Folder to save the plots into.",
        type=Path,
    )
    parser.add_argument(
        "--silent", action="store_true", help="Don't push anything to stdout."
    )
    args = parser.parse_args().__dict__


if __name__ == "__main__":
    duckcon = duckdb.connect()

    with open(args["config"], "rb") as f:
        config = tomllib.load(f)

    _names = (
        list(map(str, args["tables"])) if "names" not in config else config["names"]
    )
    _counts = {}
    for _title, _sql in config["sqls"].items():
        _ids = upsetplot.from_contents(
            {
                _name: set(
                    duckcon.query(_sql.format(table=_table))
                    .df()
                    .itertuples(index=False, name=None)
                )
                for _name, _table in zip(_names, args["tables"])
            }
        )

        upsetplot.plot(_ids, show_counts=True)
        plt.title(_title)
        plt.savefig(
            args["out"] / f"{_title}.pdf",
            bbox_inches="tight",
            **(config["plot_kwargs"] if "plot_kwargs" in config else {}),
        )
        plt.close()
        for _presence, _cnt in Counter(_ids.index).items():
            _counts[(_title, *_presence)] = _cnt

    _counts = pd.Series(_counts).reset_index()
    _counts.columns = ("sql",) + tuple(_names) + ("count",)
    _counts.to_csv(args["out"] / "counts.csv", index=False)

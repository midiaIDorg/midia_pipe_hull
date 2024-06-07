#!/usr/bin/env python3
import argparse
import json
import types
from math import inf
from pathlib import Path

import numpy as np
from pandas_ops.io import read_df
from recapuccino.misc import in_ipython

if in_ipython():
    args = types.SimpleNamespace(
        sage_config="configs/search/search_engine=sage/search_engine_version=95c2993/search_engine_config=p12f15nd.json",
        mz_recalibrated_distributions="P/refined/nodes/G6tcIIzTpfYWmMrqZWubauMUZJPS56xfpxSd3gjCfSiRSL1aOq1j06W9tp9a6l26jKoecDGO5hikQFv6eITkIcw9W4F1mSq7WUHYx2xHDGD-9yikNwoPdHm0mfo6_rCHBVAKLK4FOAEcgd9RCvyxF7AfgP0HpOD1xhywEa1yFoQeHPOwltrchoHMOKzG-NaTNkzaE3JuVHTgq0GhxaRPrAk-/5XZ9UisPTQh2i87OjTse0V_c50HMYS8hrrJsW0wQK7ajeWvv3XjOy366tASUX8vWadGd59iIqARnWRPYxkgWidGQnIluHrUDVGc0hn1Twv9HHeQTcmX9H2olXfR0A_0esE5XSa43bvQ83MostNT4SHyQYzVvZUEG7_0GshHdllSfBXpIvag5z0RrMiVBb310kQZzPq_NgIBuCdfyKTGxpR9ALrjdYhdYnQcJL5Fa6d9h1u0S_YxwwTZ_GVoAgEp/bMomUhyn8zBbK_JWGi6f8iXK3NG4gSvUEUvA_IJGUUuCIgNF_CDt87EkbKfxl3Da6MWCO0euxs9jP8DfHDykNf-hZ8nLWzTDDwM6OMDwZT7J4sCDPNkoxU27YOL9ZDKc7j8li-pcJsxOIpDhQSUvnp9fBQcNjoPbW99PspoI0e6Ac7qfAT5kCH835FwPAbfy-/HACkxWl0yuvQWA0G9Vs0AFByKK7B-pLzq7LoUIR3fLVTrZT_ZkWKtDrtWXamoO03WgSguqwBmXQhu8DK949sXfQt2f142zP8laKVtwA=/mz_recalibrated_distributions.parquet",
        lo_quantile_tag="c",
        lo_quantile=2,
        hi_quantile_tag="c",
        hi_quantile=98,
    )
else:
    parser = argparse.ArgumentParser(
        description="Upadate SAGE json config with m/z recalibration results.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "sage_config",
        help="Path to sage config to be used as template.",
        type=Path,
    )
    parser.add_argument(
        "mz_recalibrated_distributions",
        help="Path to sage config to be used as template.",
        type=Path,
    )
    parser.add_argument(
        "output",
        help="Where to save the refined SAGE search json.",
        type=Path,
    )
    parser.add_argument(
        "--lo_quantile_tag",
        choices=["d", "c", "m"],
        default="c",
        help="A letter: d for decile centile, c for centile, m for promile.",
    )
    parser.add_argument(
        "--lo_quantile",
        default=2,
        help="A value for quantile (int from 0 to 10 for decile, 100 for centile, and 1000 for promile).",
        type=int,
    )
    parser.add_argument(
        "--hi_quantile_tag",
        choices=["d", "c", "m"],
        default="c",
        help="A letter: d for decile centile, c for centile, m for promile.",
    )
    parser.add_argument(
        "--hi_quantile",
        default=98,
        help="A value for quantile (int from 0 to 10 for decile, 100 for centile, and 1000 for promile).",
        type=int,
    )
    args = types.SimpleNamespace(**parser.parse_args().__dict__)


if __name__ == "__main__":
    with open(args.sage_config, "rb") as fh:
        config = json.load(fh)

    assert args.lo_quantile >= 0
    assert args.hi_quantile >= 0

    lo_base = 10
    if args.lo_quantile_tag == "c":
        lo_base = 100
    if args.lo_quantile_tag == "m":
        lo_base = 1000

    hi_base = 10
    if args.hi_quantile_tag == "c":
        hi_base = 100
    if args.hi_quantile_tag == "m":
        hi_base = 1000

    assert args.lo_quantile <= lo_base
    assert args.hi_quantile <= hi_base

    lo_prob = args.lo_quantile / lo_base
    hi_prob = args.hi_quantile / hi_base

    assert (
        lo_prob < hi_prob
    ), f"Cannot accept: {args.lo_quantile_tag}{args.lo_quantile} >= {args.hi_quantile_tag}{args.hi_quantile}."

    mz_distributions = read_df(args.mz_recalibrated_distributions)

    ms1_lo_quantile, ms1_hi_quantile = np.interp(
        [lo_prob, hi_prob], mz_distributions.probability, mz_distributions.ms1
    )
    ms2_lo_quantile, ms2_hi_quantile = np.interp(
        [lo_prob, hi_prob], mz_distributions.probability, mz_distributions.ms2
    )

    ROUND = lambda x, d: float(round(x, d))

    config["precursor_tol"]["ppm"] = [
        ROUND(ms1_lo_quantile, 4),
        ROUND(ms1_hi_quantile, 4),
    ]
    config["fragment_tol"]["ppm"] = [
        ROUND(ms2_lo_quantile, 4),
        ROUND(ms2_hi_quantile, 4),
    ]

    with open(args.output, "w") as fh:
        json.dump(config, fh, indent=4)

#!/usr/bin/env python3

import argparse
import json
from pathlib import Path
from pprint import pprint

from recapuccino.misc import in_ipython

if in_ipython():
    args = dict(
        sage_config="configs/search/sage/run/p12f15.json",
        ms2rescore_config="configs/ms2rescore/v3.0.3/default.json",
        out="/tmp/test.json",
        silent=False,
    )
else:
    parser = argparse.ArgumentParser(
        description="Adjust the config of the ms2rescore to include PTMs from SAGE config and dump to stdout."
    )
    parser.add_argument(
        "sage_config",
        help="Path to a json config for SAGE (best if one including __MIDIA__ fields).",
        type=Path,
    )
    parser.add_argument(
        "out",
        help="Path to an output json.",
        type=Path,
    )
    parser.add_argument(
        "ms2rescore_config",
        help="Path to a json template config for ms2rescore.",
        type=Path,
    )
    parser.add_argument("--silent", help="Shut me up.", action="store_true")
    args = parser.parse_args().__dict__

if __name__ == "__main__":
    with open(args["sage_config"], "r") as f:
        sage_config = json.load(f)
        if not args["silent"]:
            pprint(sage_config)

    with open(args["ms2rescore_config"], "r") as f:
        ms2rescore_config = json.load(f)
        if not args["silent"]:
            pprint(ms2rescore_config)

    final_config = ms2rescore_config

    with open(args["out"], "w") as f:
        json.dump(final_config, f)

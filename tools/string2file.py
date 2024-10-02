#!/usr/bin/env python3

import argparse
import json

import toml

parser = argparse.ArgumentParser(
    description="Write the provided properly espaced string to file."
)
parser.add_argument(
    "str",
    help="A string with something to save fo file.",
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

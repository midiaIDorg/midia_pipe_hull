#!/usr/bin/env python3
import argparse
import os
import types
from pathlib import Path
from types import SimpleNamespace

import toml

from snakemaketools.io_ops import get_wished_inputs_and_outputs

if True:
    from warnings import warn
    from IPython import get_ipython

    warn("Development mode")
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")

    args = SimpleNamespace(
        pipeline_script=Path("configs/pipelines/base/base.py"),
        forward_rules=Path("configs/forward_rules.py"),
        out_paths=Path("/tmp/paths.toml"),
        config=Path("configs/pipelines/base/matteo.toml"),
        pipeline_output_folder=Path("/tmp/test"),
        wildcard_diffs_serialized="dataset=G8027/calibration=G8045",
        silent=False,
    )
else:
    parser = argparse.ArgumentParser(description="Get pipeline paths.")
    parser.add_argument(
        "pipeline_script",
        help="A path to a python script defining paths.",
        type=Path,
    )
    parser.add_argument(
        "forward_rules",
        help="A path to pipeline agnostic forward path template rules.",
        type=Path,
    )
    parser.add_argument(
        "out_paths",
        help="Where to save the filled paths.",
        type=Path,
    )
    parser.add_argument(
        "config",
        help="Path to a toml config with potentially [wildcard_diffs] and required [wishlist].",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "pipeline_output_folder",
        help="Path to the final location [relative to midia_pipe].",
        type=Path,
    )
    parser.add_argument(
        "--wildcard_diffs_serialized",
        help="Serialized wildcard diffs in form of '<param_name_0>=<param_value_0>/.../<param_name_{k-1}>=<param_value_{k-1}>/'. Take precedence over defaults and --wildcard_diffs_toml",
        type=str,
        default=None,
    )
    parser.add_argument(
        "--silent",
        help="Refrain from pushing to stdout.",
        action="store_true",
    )
    args = SimpleNamespace(**parser.parse_args().__dict__)


if __name__ == "__main__":
    path_template_copy_from_to = get_wished_inputs_and_outputs(
        forward_rules_path=args.forward_rules,
        config_path=args.config,
        pipeline_script_path=args.pipeline_script,
        wildcard_diffs_serialized=args.wildcard_diffs_serialized,
        pipeline_output_folder=args.pipeline_output_folder,
        silent=args.silent,
    )

    with open(args.out_paths, "w") as f:
        toml.dump(, f)

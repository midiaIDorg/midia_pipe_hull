#!/usr/bin/env python3
import argparse
from collections import Counter
from pathlib import Path
from pprint import pprint

import duckdb
import pandas as pd
import tomllib

pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", 5)

args = dict(
    source="/home/matteo/Projects/midia/pipelines/fishy_test/midia_pipe/outputs/regression_jenses_tims_settings_rescoring/G8027/G8045/Human_2024_02_16_UniProt_Taxon9606_Reviewed_20434entries_contaminant_tenzer/Human_2024_02_16_UniProt_Taxon9606_Reviewed_20434entries_contaminant_tenzer/p12f15_1psm/p12f15_1psm/baseEdgeStats^positiveEllipse^maxRank=12_depleted/ms2rescore/v3.0.3/default/rescoring/compomics/results/ms2rescore.mokapot.peptides.txt",
    config="configs/search/output_filters/sage/default.toml",
    replace_in="peptide_q",
    replace_out='"mokapot q-value',
)

parser = argparse.ArgumentParser(
    description="Make statistics from an ms2rescore run on SAGE input."
)
parser.add_argument("source", help="Path to raw SAGE results.", type=Path)
parser.add_argument(
    "config",
    help="Path to a toml config specifying the FDR filter (field [filter]).",
    type=Path,
)
parser.add_argument("output", help="Path to output.", type=Path)
parser.add_argument(
    "--replace_in", help="String to be replaced in the config.", default="peptide_q"
)
parser.add_argument(
    "--replace_out",
    help="Replacement for the string to be replaced.",
    default='"mokapot q-value',
)
args = parser.parse_args()

if __name__ == "__main__":
    with open(args["config"], "rb") as f:
        config = tomllib.load(f)
        if "replace_in" in args and "replace_out" in args:
            config["filter"] = config["filter"].replace(
                args["replace_in"], args["replace_out"]
            )
        pprint(config)

    filtered_df = duckdb.query(config["filter"].format(source=args["source"])).df()

    ms1_ClusterIDs = {int(header.split(".")[1]) for header in filtered_df.spectrum_id}
    pattern = re.compile(r"\[.*?\]")
    stripped_peptide_sequence_counts = Counter(
        pattern.sub("", pept) for pept in filtered_df.peptide
    )

    stats = {
        "proteins_in_protein_groups": len(
            {
                protein
                for protein_group in filtered_df.protein_list
                for protein in protein_group.split(",")
            }
        ),
        "protein_groups": len(Counter(filtered_df.protein_list)),
        "peptide_sequences_stripped_of_mods": len(stripped_peptide_sequence_counts),
        "peptides_with_mods": len(filtered_df.peptide.unique()),
        "charged_peptides_with_mods": len(
            filtered_df[["peptide", "charge"]].drop_duplicates()
        ),
        "phosphopeptides": (
            filtered_df["peptide"]
            .drop_duplicates()
            .str.contains("79.9663", na=False)
            .sum()
        ),
        "MS1_ClusterID": len(ms1_ClusterIDs),
    }
    pd.DataFrame([stats]).to_csv(args["output"], index=False)

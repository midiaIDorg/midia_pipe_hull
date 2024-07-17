#!/usr/bin/env python3
import argparse
import re
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
    description="Make statistics from an ms2rescore run on SAGE input.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("source", help="Path to raw SAGE results.", type=Path)
parser.add_argument("output", help="Path to output.", type=Path)
parser.add_argument(
    "--filter",
    help="SQL specifying a filter.",
    default=""" 
    SELECT *
    FROM read_csv('{source}', delim="\t")
    WHERE 
    "mokapot q-value" < 0.01
    AND
    protein_list NOT LIKE '%rev_%'
    """,
)
args = parser.parse_args().__dict__

if __name__ == "__main__":
    filtered_df = duckdb.query(args["filter"].format(source=args["source"])).df()

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

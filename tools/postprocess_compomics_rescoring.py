#!/usr/bin/env python3
import argparse
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from midia_search_engines.sage_ops import (
    get_proteins_in_protein_groups,
    parse_MS1_ClusterIDs,
)
from pandas_ops.io import read_df
from recapuccino.misc import in_ipython

if in_ipython():
    from warnings import warn
    from IPython import get_ipython
    from subprocess import run
    import pandas as pd

    warn("Development mode")
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", 2)

    _folder = Path("out/base/debug_node_refinement_sage_postprocessing")
    run(["snakemake", "-call", _folder])

    _folder2 = (
        Path("out/base/default") / "sage" / "second_gen_rescored" / "tims2rescore"
    )


else:
    parser = argparse.ArgumentParser(description="Run SAGE search.")
    parser.add_argument(
        "mgf",
        help="Path to the MGF to search.",
        type=Path,
    )
    parser.add_argument(
        "fasta",
        help="Path to the fasta to search with.",
        type=Path,
    )
    parser.add_argument(
        "config",
        help="Path to a json SAGE config. It should have a field __MIDIA__ to assert things are done as intended.",
        type=Path,
    )
    parser.add_argument(
        "sage",
        help="Path to SAGE executable.",
        type=Path,
    )
    parser.add_argument(
        "unimod",
        help="Path to unimod csv with PTMs.",
        type=Path,
    )
    parser.add_argument(
        "-o",
        "--output_folder",
        help="Path to where to drop results of SAGE search.",
        type=Path,
        required=True,
    )
    parser.add_argument(
        "--silent",
        help="Be silent.",
        action="store_true",
    )
    args = parser.parse_args()

args = types.SimpleNamespace(
    found_precursors=_folder2 / "results_sage.parquet",
    found_fragments=_folder / "matched_fragments_sage_parquet.parquet",
    fragment_stats=_folder / "fragment_stats.parquet",
    edges=_folder / "hard_filtered_rough_matches.startrek",
    config=Path("configs/search/sage/postprocessing/sagePost/default.toml"),
    out_precursors=out / "outprecursors.parquet",
    out_fragments=out / "outfragments.parquet",
    out_edges=out / "edges.startrek",
    # out_stats=Path("/tmp/sagestats.csv"),
    out_QC=out / "QC_stats",
    disable_checks=False,
    silent=False,
)

# peptides = pd.read_table(_folder2 / "results.sage.ms2rescore.mokapot.peptides.txt")


# psms_mokapot = pd.read_table(_folder2 / "results.sage.ms2rescore.mokapot.psms.txt")
# proteins = pd.read_table(_folder2 / "results.sage.ms2rescore.mokapot.proteins.txt")

# peptides["MS1_ClusterID"] = parse_MS1_ClusterIDs(peptides.spectrum_id)
# peptides = peptides.sort_values("MS1_ClusterID", ignore_index=True)

psms = pd.read_table(_folder2 / "results.sage.ms2rescore.psms.tsv")
psms["MS1_ClusterID"] = parse_MS1_ClusterIDs(psms.spectrum_id)
psms = psms.sort_values("MS1_ClusterID", ignore_index=True)

psms


def clean_protein_list(x):
    y = x[1:-1].replace("'", "").split(", ")
    y.sort()
    return tuple(y)


found_protein_groups = set(map(clean_protein_list, psms["protein_list"]))
len(found_protein_groups) == len(psms["protein_list"])

protein_in_protein_groups = set(
    protein for protein_group in found_protein_groups for protein in protein_group
)


# sage_peptides = read_df(_folder / "results_sage.parquet")
# sage_fragments = read_df(_folder / "matched_fragments_sage_parquet.parquet")

# psms_mokapot["mokapot q-value"].min()


MS1_ClusterID_counts = Counter(peptides.MS1_ClusterID)
{k: v for k, v in MS1_ClusterID_counts.items() if v > 1}
len(MS1_ClusterID_counts)


get_proteins_in_protein_groups(proteins[""])

stats = {
    "dataset_description": "second_gen_rescored",
    "proteins_in_protein_groups": len(protein_in_protein_groups),
    "protein_groups": len(found_protein_groups),
    "peptide_sequences_stripped_of_mods": 0,
    "peptides_with_mods": 0,
    "charged_peptides_with_mods": 0,
    "unique_y_fragments": 0,
    "unique_b_fragments": 0,
    "edges": 0,
    "edges_with_multiply_annotated_fragments": 0,
    "precursors_with_multiply_annotated_fragments": 0,
    "MS1_Clusters": len(psms.MS1_ClusterID.unique()),
    # "MS1_Clusters_with_multiply_annotated_fragments": 0,
}

#!/usr/bin/env python3
import argparse
import json
import re
from collections import Counter
from pathlib import Path

import networkx as nx

import duckdb
import matplotlib.pyplot as plt
import pandas as pd
from midia_search_engines.sage_ops import get_proteins_in_protein_groups
from pandas_ops.io import read_df, save_df
from pandas_ops.misc import in_ipython
from pandas_ops.sortedness import count_sorted

if in_ipython():
    from IPython import get_ipython

    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
    pd.set_option("display.max_columns", None)
    pd.set_option("display.max_rows", 10)
    from types import SimpleNamespace

    args = SimpleNamespace(
        filtered_results="P/search/sagepost/W2WsA8Bx4MbKvGaEv1FUvWz5_WxWdhCIeH3mVWTeBP6us1jsguz_v1dNQxzK8Zi8dV_GmiBzvv8yPOnWnvKYWiw36PWiZN9yQbrILkC7nFzx7U7sq0ImLiEvOAKEbbd_ZnLlXuHvh5-nVe4-/ZV8WqQlEMJKAlewomiQQtouxQlEP9f8UYbhbUVza4SFvGjKFiGGilDlnI18BTdrt8f57xDY33cfDsWUpZeqTOnJCw8HDxRhMSuCxhJx8mjxMjiwfFiM9F4ecLE7sKba-nes-SVXY8RxLNV1q9ADHvH0e-u78sXWKRFmwx0O4k4j2QAjqI_n1FrZo3W3EMiLOeCgGOn0ox3BGPmBV7pwaq5I-/jtSZGf2LdQTI2bxGE6EeGbLpjbItzrGYTJmUWVnbmo2oylkRMhy4TWwWQG9BAxAVzSWUwjOTqihk8VZ8fIkCMnRQveMYhyupwzhGTr2IrH-n6cQniurdyQG4DKRStrb1ZSETl1jmQNkWu8i7DwtOmZP0WeEgY4Gv00iTmtGfqz_jkOmXb4NbzkYz0hX6YY9MT8YTffzIM1b_6L527DJluH9EUXDKfNCafBTPTnqlCzUV9-zz1Huxsksup0Yvl-/MN4YMHsqU5sCXcIgDQuHA5NGrpzll0Hw4FAKgstd3JrO3hc23x1SEoj2h-PLsHBG1-zl0JXQsrzKjRaAPAkY6DTmoilEg878SLVa9mmGN1t_I0DIkwxLFLWTI0RDoigFDNQFFgsLEGJd9bPHHCwn8dEYDw98DQ0JQI4PHOToF6E0CiDYDQNdVfmkXjtSHA4epab3z7Zte-Iiht4TtkWAjshN1gji_vBcwgwqBtvI9tRABvRgG73qlPkTMs_Q-/sUR7yKowms1w8HTgWjwb8IiwDf4UEL4kAvqHvKdAXrdgQbwI=/filtered.results.sage.parquet",
        filtered_matched_fragments="P/search/sagepost/W2WsA8Bx4MbKvGaEv1FUvWz5_WxWdhCIeH3mVWTeBP6us1jsguz_v1dNQxzK8Zi8dV_GmiBzvv8yPOnWnvKYWiw36PWiZN9yQbrILkC7nFzx7U7sq0ImLiEvOAKEbbd_ZnLlXuHvh5-nVe4-/ZV8WqQlEMJKAlewomiQQtouxQlEP9f8UYbhbUVza4SFvGjKFiGGilDlnI18BTdrt8f57xDY33cfDsWUpZeqTOnJCw8HDxRhMSuCxhJx8mjxMjiwfFiM9F4ecLE7sKba-nes-SVXY8RxLNV1q9ADHvH0e-u78sXWKRFmwx0O4k4j2QAjqI_n1FrZo3W3EMiLOeCgGOn0ox3BGPmBV7pwaq5I-/jtSZGf2LdQTI2bxGE6EeGbLpjbItzrGYTJmUWVnbmo2oylkRMhy4TWwWQG9BAxAVzSWUwjOTqihk8VZ8fIkCMnRQveMYhyupwzhGTr2IrH-n6cQniurdyQG4DKRStrb1ZSETl1jmQNkWu8i7DwtOmZP0WeEgY4Gv00iTmtGfqz_jkOmXb4NbzkYz0hX6YY9MT8YTffzIM1b_6L527DJluH9EUXDKfNCafBTPTnqlCzUV9-zz1Huxsksup0Yvl-/MN4YMHsqU5sCXcIgDQuHA5NGrpzll0Hw4FAKgstd3JrO3hc23x1SEoj2h-PLsHBG1-zl0JXQsrzKjRaAPAkY6DTmoilEg878SLVa9mmGN1t_I0DIkwxLFLWTI0RDoigFDNQFFgsLEGJd9bPHHCwn8dEYDw98DQ0JQI4PHOToF6E0CiDYDQNdVfmkXjtSHA4epab3z7Zte-Iiht4TtkWAjshN1gji_vBcwgwqBtvI9tRABvRgG73qlPkTMs_Q-/sUR7yKowms1w8HTgWjwb8IiwDf4UEL4kAvqHvKdAXrdgQbwI=/filtered.matched_fragments.sage.parquet",
        output="/tmp/stats.csv",
    )

else:
    parser = argparse.ArgumentParser(description="Make statistics of SAGE search.")
    parser.add_argument(
        "filtered_results",
        help="Path to filtered SAGE results (likely after FDR filter)",
        type=Path,
    )
    parser.add_argument(
        "filtered_matched_fragments",
        help="Path to mapped back and filtered fragments.",
        type=Path,
    )
    parser.add_argument("output", help="Path to output.", type=Path)
    args = parser.parse_args()


if __name__ == "__main__":
    sage = read_df(args.filtered_results)

    stats = {
        "proteins_in_protein_groups": 0,
        "protein_groups": 0,
        "peptides_stripped_of_mods": 0,
        "peptides_with_mods": 0,
        "edges_unique_annotation": 0,
        "edges_multiple_annotation": 0,
        "MS1_Clusters": 0,
        "MS2_Clusters": 0,
        "fragments": "",
    }

    if len(sage) > 0:
        pattern = re.compile(r"\[.*?\]")
        stripped_peptide_sequence_counts = Counter(
            pattern.sub("", pept) for pept in sage.peptide
        )

        stats["proteins_in_protein_groups"] = len(
            get_proteins_in_protein_groups(sage.proteins)
        )
        stats["protein_groups"] = len(sage.proteins)
        stats["peptides_stripped_of_mods"] = len(stripped_peptide_sequence_counts)
        stats["peptides_with_mods"] = len(sage.peptide.unique())
        stats["charged_peptides_with_mods"] = len(
            sage[["peptide", "charge"]].drop_duplicates()
        )

    fragments = read_df(args.filtered_matched_fragments)
    if len(fragments):
        stats["MS1_Clusters"] = count_sorted(fragments.MS1_ClusterID)
        stats["MS2_Clusters"] = len(set(fragments.MS2_ClusterID))

        stats["edges_unique_annotation"] = len(
            set(zip(fragments.MS1_ClusterID, fragments.MS2_ClusterID))
        )
        stats["edges_multiple_annotation"] = (
            len(fragments) - stats["edges_unique_annotation"]
        )

        stats["fragments"] = json.dumps(dict(Counter(fragments.fragment_type)))

    save_df(pd.DataFrame([stats]), args.output)


# df = read_stats(args.results_sage_tsv_paths[0], args.sql)

# G = nx.Graph()
# peptides = set({})
# proteins = set({})
# for peptide, proteins_to_split in zip(df.peptide, df.proteins):
#     peptides.add(peptide)
#     for protein in proteins_to_split.split(";"):
#         proteins.add(protein)
#         G.add_edge(peptide, protein)

# H = nx.Graph()
# # get all peptides mapping to a single protein only
# # get list of source proteins of those peptides -> protein_list_w_unique
# # remove all peptides mapping to protein_list_w_unique
# # remaining peptides are "protein_group_specific", i.e. map only to (two or more proteins) that are not in the protein_list_w_unique
# # sequentially find other protein_entries that explain most of the remaining peptides


# pos = nx.spring_layout(G, seed=3113794652)  # positions for all nodes
# options = {"edgecolors": "tab:gray", "node_size": 100, "alpha": 0.9}
# nx.draw_networkx_nodes(
#     G, pos, nodelist=list(all_peptides), node_color="tab:red", **options
# )
# nx.draw_networkx_nodes(
#     G, pos, nodelist=list(all_proteins), node_color="tab:blue", **options
# )
# nx.draw_networkx_edges(G, pos, width=1.0, alpha=0.5)
# plt.show()

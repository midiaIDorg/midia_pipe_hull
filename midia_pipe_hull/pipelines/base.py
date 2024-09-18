"""
I guess we can assume that pipelines will be simply importable functions.

snakemaketools: general long snake capabilities.
"""
from midia_pipe_hull.pipeline_rules import *
from snakemaketools.datastructures import DotDict
from snakemaketools.models import Node


def pipeline(
    raw_data: Node,
    precursor_clustering_config: Node,
    fragment_clustering_config: Node,
    precursor_cluster_stats_config: Node,
    fragment_cluster_stats_config: Node,
    matching_config: Node,
    # defaults
    config_baseline_removal: Node | None = None,  # not passed
) -> DotDict:
    N = DotDict(**locals())  # N stands for Nodes.

    if config_baseline_removal is not None:
        N.raw_data = remove_rawdata_baseline(N.raw_data, N.config_baseline_removal)

    (
        N.precursors,
        N.precursor_clustering_stdout,
        N.precursor_clustering_stderr,
    ) = cluster_precursors(N.raw_data, N.precursor_clustering_config)

    (
        N.fragments,
        N.fragment_clustering_stdout,
        N.fragment_clustering_stderr,
    ) = cluster_fragments(N.raw_data, N.fragment_clustering_config)

    N.precursor_stats = get_cluster_stats(
        N.precursors,
        N.precursor_cluster_stats_config,
    )

    N.fragment_stats = get_cluster_stats(
        N.fragments,
        N.fragment_cluster_stats_config,
    )

    N.rough_matches = match_precursors_and_fragments(
        N.precursor_stats,
        N.fragment_stats,
        N.matching_config,
    )

    return N  # Nodes: paths ids.

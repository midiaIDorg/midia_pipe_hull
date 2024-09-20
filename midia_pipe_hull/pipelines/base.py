"""
I guess we can assume that pipelines will be simply importable functions.

snakemaketools: general long snake capabilities.
"""
from midia_pipe_hull.pipeline_rules import *
from snakemaketools.datastructures import DotDict
from snakemaketools.models import Path


def pipeline(
    subconfigs: dict,
    dataset: str,
    fasta: str,
    # defaults
    calibration: str = "",  # "" == using Bruker windows
) -> DotDict:
    paths = DotDict()
    register_subconfigs(paths, subconfigs)

    paths.dataset, paths.dataset_tdf, paths.dataset_tdf_bin = register_tdf_rawdata(
        dataset
    )
    (
        paths.calibration,
        paths.calibration_tdf,
        paths.calibration_tdf_bin,
    ) = register_tdf_rawdata(calibration)

    paths.fasta = register_fasta(fasta)

    paths.dataset_analysis_tdf_hash = hash256(paths.dataset_tdf)
    paths.dataset_analysis_tdf_bin_hash = hash256(paths.dataset_tdf_bin)
    paths.calibration_analysis_tdf_hash = hash256(paths.calibration_tdf)
    paths.calibration_analysis_tdf_bin_hash = hash256(paths.calibration_tdf_bin)

    # if "config_baseline_removal" in subconfigs:
    #     paths.dataset = remove_rawdata_baseline(
    #         paths.dataset, paths.config_baseline_removal
    #     )

    # (
    #     paths.precursors,
    #     paths.precursor_clustering_stdout,
    #     paths.precursor_clustering_stderr,
    # ) = cluster_precursors(paths.dataset, paths.precursor_clustering_config)

    # (
    #     paths.fragments,
    #     paths.fragment_clustering_stdout,
    #     paths.fragment_clustering_stderr,
    # ) = cluster_fragments(paths.dataset, paths.fragment_clustering_config)

    # paths.precursor_stats = get_cluster_stats(
    #     paths.precursors,
    #     paths.precursor_cluster_stats_config,
    # )

    # paths.fragment_stats = get_cluster_stats(
    #     paths.fragments,
    #     paths.fragment_cluster_stats_config,
    # )

    # paths.rough_matches = match_precursors_and_fragments(
    #     paths.precursor_stats,
    #     paths.fragment_stats,
    #     paths.matching_config,
    # )

    return paths

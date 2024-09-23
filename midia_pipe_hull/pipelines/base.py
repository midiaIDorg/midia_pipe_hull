"""
I guess we can assume that pipelines will be simply importable functions.

snakemaketools: general long snake capabilities.
"""
from midia_pipe_hull.pipeline_rules import *
from snakemaketools.datastructures import DotDict


def fill_DB_with_paths(
    subconfigs: dict,
    dataset: str,
    fasta: str,
    # defaults
    calibration: str = "",  # "" == using Bruker windows
) -> DotDict:
    """
    Remember that here we only register paths in the DB.
    That does not automatically mean that all of these paths will ever be created.
    """
    paths = DotDict()

    for subconfig_type, subconfig in subconfigs.items():
        assert_subconfig_is_valid(subconfig_type, subconfig)

    for subconfig_type, subconfig in subconfigs.items():
        paths[subconfig_type] = get_subconfig(
            subconfig_type,
            subconfig,
        )

    paths.update(register_fasta(fasta))  # DotDicts all the way down

    (
        paths.dataset,
        paths.dataset_analysis_tdf,
        paths.dataset_analysis_tdf_bin,
    ) = register_tdf_rawdata(dataset)

    paths.dataset_analysis_tdf_hash = hash256(paths.dataset_analysis_tdf)
    paths.dataset_analysis_tdf_bin_hash = hash256(paths.dataset_analysis_tdf_bin)
    paths.dataset_marginals_plots = raw_data_marginals_plots_folder(paths.dataset)

    if calibration:
        (
            paths.calibration,
            paths.calibration_analysis_tdf,
            paths.calibration_analysis_tdf_bin,
        ) = register_tdf_rawdata(calibration)

        paths.calibration_analysis_tdf_hash = hash256(paths.calibration_analysis_tdf)
        paths.calibration_analysis_tdf_bin_hash = hash256(
            paths.calibration_analysis_tdf_bin
        )

        paths.dataset_matches_calibration_assertion = (
            report_if_dataset_and_calibration_comply(
                dataset=paths.dataset,
                calibration=paths.calibration,
            )
        )
        paths.calibration_marginals_plots = raw_data_marginals_plots_folder(
            paths.calibration
        )

    if "baseline_removal_config" in subconfigs:
        (
            paths.dataset,
            paths.dataset_analysis_tdf,
            paths.dataset_analysis_tdf_bin,
        ) = remove_raw_data_baseline(
            raw_data=paths.dataset,
            config=paths.baseline_removal_config,
        )

    if "tims" in subconfigs["precursor_clustering_config"]["software"]:
        tims_executable = get_tims_executable(
            subconfig=subconfigs["precursor_clustering_config"]
        )
        (
            paths.precursor_clusters_hdf,
            paths.precursor_clustering_qc,
        ) = cluster_precursors_with_tims(
            executable=tims_executable,
            dataset=paths.dataset,
            config=paths.precursor_clustering_config,
        )
        (
            paths.precursor_clusters,
            paths.additional_precursor_cluster_stats,
        ) = postprocess_fragment_tims_clusters(
            clusters_hdf=paths.precursor_clusters_hdf,
            analysis_tdf=paths.dataset_analysis_tdf,
        )

    if "tims" in subconfigs["fragment_clustering_config"]["software"]:
        tims_executable = get_tims_executable(
            subconfig=subconfigs["precursor_clustering_config"]
        )
        (
            paths.fragment_clusters_hdf,
            paths.fragment_clustering_qc,
        ) = cluster_fragments_with_tims(
            executable=tims_executable,
            dataset=paths.dataset,
            config=paths.fragment_clustering_config,
        )
        (
            paths.fragment_clusters,
            paths.additional_fragment_cluster_stats,
        ) = postprocess_fragment_tims_clusters(
            clusters_hdf=paths.fragment_clusters_hdf,
            analysis_tdf=paths.dataset_analysis_tdf,
        )

    # paths.precursor_stats = get_cluster_stats(
    #     precursor_clusters=paths.precursors,
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

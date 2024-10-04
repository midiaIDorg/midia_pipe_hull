"""
I guess we can assume that pipelines will be simply importable functions.

snakemaketools: general long snake capabilities.
"""
from snakemaketools.datastructures import DotDict


def get_nodes(
    rules: DotDict,
    configs: DotDict,
    dataset: str,
    fasta: str,
    calibration: str = "",  # "" == using Bruker windows
) -> DotDict:
    """
    Remember that here we only register paths in the DB.
    That does not automatically mean that all of these paths will ever be created.
    """
    nodes = DotDict()

    nodes.fasta = rules.set_fasta()

    (
        nodes.dataset,
        nodes.dataset_analysis_tdf,
        nodes.dataset_analysis_tdf_bin,
    ) = rules.set_dataset()

    nodes.dataset_analysis_tdf_hash = rules.hash256(path=nodes.dataset_analysis_tdf)
    nodes.dataset_analysis_tdf_bin_hash = rules.hash256(
        path=nodes.dataset_analysis_tdf_bin
    )
    nodes.dataset_marginal_distributions = rules.get_marginal_distribution_of_raw_data(
        raw_data=nodes.dataset
    )

    if calibration:
        (
            nodes.calibration,
            nodes.calibration_analysis_tdf,
            nodes.calibration_analysis_tdf_bin,
        ) = rules.set_calibration()

        nodes.calibration_analysis_tdf_hash = rules.hash256(
            path=nodes.calibration_analysis_tdf
        )
        nodes.calibration_analysis_tdf_bin_hash = rules.hash256(
            path=nodes.calibration_analysis_tdf_bin
        )

        nodes.dataset_matches_calibration_assertion = (
            rules.report_if_dataset_and_calibration_comply(
                dataset=nodes.dataset,
                calibration=nodes.calibration,
            )
        )
        nodes.calibration_marginal_distributions = (
            rules.get_marginal_distribution_of_raw_data(raw_data=nodes.calibration)
        )

    if "baseline_removal" in configs:
        nodes.baseline_removal_config = rules.baseline_removal_config.set(
            configs.baseline_removal
        )
        (
            nodes.dataset,
            nodes.dataset_analysis_tdf,
            nodes.dataset_analysis_tdf_bin,
        ) = rules.remove_raw_data_baseline(
            raw_data=nodes.dataset,
            config=nodes.baseline_removal_config,
        )

    if "tims_precursor_clusterer" in configs:
        nodes.tims_precursor_clusterer = rules.tims_precursor_clusterer.set(
            config=configs.tims_precursor_clusterer
        )
        if "tims_precursor_clusterer_config" in configs:
            nodes.tims_precursor_clusterer_config = (
                rules.tims_precursor_clusterer_config.set(
                    configs.tims_precursor_clusterer_config,
                )
            )
            (
                nodes.precursor_clusters_hdf,
                nodes.precursor_clustering_qc,
                nodes.additional_precursor_cluster_stats,
            ) = rules.tims_cluster_precursors(
                dataset=nodes.dataset,
                config=nodes.tims_precursor_clusterer_config,
                executable=nodes.tims_precursor_clusterer,
            )
            nodes.precursor_clusters = rules.postprocess_tims_precursor_clusters(
                clusters_hdf=nodes.precursor_clusters_hdf,
                analysis_tdf=nodes.dataset_analysis_tdf,
            )

    if "tims_fragment_clusterer" in configs:
        nodes.tims_fragment_clusterer = rules.tims_fragment_clusterer.set(
            config=configs.tims_fragment_clusterer
        )
        if "tims_fragment_clusterer_config" in configs:
            nodes.tims_fragment_clusterer_config = (
                rules.tims_fragment_clusterer_config.set(
                    configs.tims_fragment_clusterer_config
                )
            )
            (
                nodes.fragment_clusters_hdf,
                nodes.fragment_clustering_qc,
                nodes.additional_fragment_cluster_stats,
            ) = rules.tims_cluster_fragments(
                dataset=nodes.dataset,
                config=nodes.tims_fragment_clusterer_config,
                executable=nodes.tims_fragment_clusterer,
            )
            nodes.fragment_clusters = rules.postprocess_tims_fragment_clusters(
                clusters_hdf=nodes.fragment_clusters_hdf,
                analysis_tdf=nodes.dataset_analysis_tdf,
            )

    nodes.precursor_cluster_stats_config = rules.precursor_cluster_stats_config.set(
        configs.precursor_cluster_stats_config
    )
    nodes.precursor_cluster_stats = rules.get_precursor_cluster_stats(
        clusters=nodes.precursor_clusters,
        config=nodes.precursor_cluster_stats_config,
    )

    if "tims_additional_precursor_cluster_stats" in nodes:
        nodes.precursor_cluster_stats = rules.merge_additional_tims_precursor_stats(
            cluster_stats=nodes.precursor_cluster_stats,
            additional_stats=nodes.additional_precursor_cluster_stats,
        )

    nodes.fragment_cluster_stats_config = rules.fragment_cluster_stats_config.set(
        configs.fragment_cluster_stats_config
    )
    nodes.fragment_cluster_stats = rules.get_fragment_cluster_stats(
        clusters=nodes.fragment_clusters,
        config=nodes.fragment_cluster_stats_config,
    )

    if "tims_additional_fragment_cluster_stats" in nodes:
        nodes.fragment_cluster_stats = rules.merge_additional_tims_fragment_stats(
            cluster_stats=nodes.fragment_cluster_stats,
            additional_stats=nodes.additional_fragment_cluster_stats,
        )

    nodes.matching_config = rules.matching_config.set(configs.matching_config)
    nodes.rough_matches = rules.match_precursors_and_fragments(
        precursor_cluster_stats=nodes.precursor_cluster_stats,
        fragment_cluster_stats=nodes.fragment_cluster_stats,
        matching_config=nodes.matching_config,
    )

    return nodes

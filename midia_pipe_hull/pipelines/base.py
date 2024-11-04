"""
I guess we can assume that pipelines will be simply importable functions.

snakemaketools: general long snake capabilities.
"""
import snakemaketools.rules
from snakemaketools.datastructures import DotDict

# import Config, Node, Rule, Wildcard


def get_nodes(
    rules: DotDict[str, snakemaketools.rules.Rule],
    configs: DotDict[str, snakemaketools.rules.Config],
    wildcards: DotDict[str, snakemaketools.rules.Wildcard],
) -> DotDict[str, snakemaketools.rules.Node]:
    """
    Remember that here we only register paths in the DB.
    That does not automatically mean that all of these paths will ever be created.
    """
    nodes: DotDict[str, snakemaketools.rules.Node] = DotDict()

    nodes.fasta = rules.get_stored_fasta(fasta=wildcards.fasta)

    (
        nodes.dataset,
        nodes.dataset_analysis_tdf,
        nodes.dataset_analysis_tdf_bin,
    ) = rules.fetch_data(folder_d=wildcards.dataset)

    nodes.dataset_analysis_tdf_hash = rules.hash256(path=nodes.dataset_analysis_tdf)
    nodes.dataset_analysis_tdf_bin_hash = rules.hash256(
        path=nodes.dataset_analysis_tdf_bin
    )
    nodes.dataset_marginal_distribution_plots = rules.get_marginal_distribution_plots(
        raw_data=nodes.dataset
    )

    if "calibration" in wildcards:
        (
            nodes.calibration,
            nodes.calibration_analysis_tdf,
            nodes.calibration_analysis_tdf_bin,
        ) = rules.fetch_data(folder_d=wildcards.calibration)

        nodes.calibration_analysis_tdf_hash = rules.hash256(
            path=nodes.calibration_analysis_tdf
        )
        nodes.calibration_analysis_tdf_bin_hash = rules.hash256(
            path=nodes.calibration_analysis_tdf_bin
        )

        nodes.dataset_matches_calibration_assertion = (
            rules.report_if_dataset_and_calibration_comply(
                dataset_analysis_tdf=nodes.dataset_analysis_tdf,
                calibration_analysis_tdf=nodes.calibration_analysis_tdf,
            )
        )
        nodes.calibration_marginal_distributions = (
            rules.get_marginal_distribution_plots(raw_data=nodes.calibration)
        )

    if "baseline_removal" in configs:
        # TO THINK: do we actually need to use the `set` method?
        # No, likely need one method in the pipeline to set the configs.
        nodes.baseline_removal_config = rules.get_config_from_db_into_file_system(
            config=configs.baseline_removal
        )

        (
            nodes.dataset,
            nodes.dataset_analysis_tdf,
            nodes.dataset_analysis_tdf_bin,
        ) = rules.remove_raw_data_baseline(
            raw_data=nodes.dataset,
            config=nodes.baseline_removal_config,
        )

    if configs.precursor_clusterer.location_wildcards.software == "tims":
        nodes.tims_precursor_clusterer_config = (
            rules.get_config_from_db_into_file_system(
                config=configs.precursor_clusterer
            )
        )

        _level = snakemaketools.rules.Wildcard(name="level", value="precursor")
        _version = configs.precursor_clusterer.location_wildcards.version
        (
            nodes.precursor_clusters_hdf,
            nodes.precursor_clustering_stdout,
            nodes.precursor_clustering_stderr,
        ) = rules.cluster_with_tims(
            dataset=nodes.dataset,
            config=nodes.tims_precursor_clusterer_config,
            level=_level,
            version=_version,
        )
        (
            nodes.precursor_clusters,
            nodes.additional_precursor_cluster_stats,
        ) = rules.postprocess_precursor_tims_clusters(
            clusters_hdf=nodes.precursor_clusters_hdf,
            analysis_tdf=nodes.dataset_analysis_tdf,
            level=_level,
            version=_version,
        )

    if configs.fragment_clusterer.location_wildcards.software == "tims":
        nodes.tims_fragment_clusterer_config = (
            nodes.tims_precursor_clusterer_config
        ) = rules.get_config_from_db_into_file_system(config=configs.fragment_clusterer)

        _level = snakemaketools.rules.Wildcard(name="level", value="fragment")
        _version = configs.fragment_clusterer.location_wildcards.version
        (
            nodes.fragment_clusters_hdf,
            nodes.fragment_clustering_stdout,
            nodes.fragment_clustering_stderr,
        ) = rules.cluster_with_tims(
            dataset=nodes.dataset,
            config=nodes.tims_fragment_clusterer_config,
            level=_level,
            version=_version,
        )
        (
            nodes.fragment_clusters,
            nodes.additional_fragment_cluster_stats,
        ) = rules.postprocess_tims_fragment_clusters(
            clusters_hdf=nodes.fragment_clusters_hdf,
            analysis_tdf=nodes.dataset_analysis_tdf,
            level=_level,
            version=_version,
        )

    nodes.precursor_cluster_stats_config = rules.get_config_from_db_into_file_system(
        config=configs.precursor_cluster_stats_config
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

    nodes.fragment_cluster_stats_config = rules.get_config_from_db_into_file_system(
        config=configs.fragment_cluster_stats_config
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

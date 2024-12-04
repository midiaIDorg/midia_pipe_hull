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
    nodes.memmapped_dataset = rules.memmap_data(folder_d=nodes.dataset)

    if "calibration" in wildcards and wildcards.calibration.value is not None:
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

        nodes.memmapped_calibration = rules.memmap_data(folder_d=nodes.calibration)

        nodes.calibration_results = rules.precompute_calibration(
            calibration=nodes.calibration,
            memmapped_calibration=nodes.memmapped_calibration,
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

        (
            nodes.precursor_clusters_hdf,
            nodes.precursor_clustering_stdout,
            nodes.precursor_clustering_stderr,
        ) = rules.cluster_with_tims(
            dataset=nodes.dataset,
            config=nodes.tims_precursor_clusterer_config,
            level=snakemaketools.rules.Wildcard(
                name="level", value="precursor"
            ),  # passing a not-user-defined wildcard
            version=configs.precursor_clusterer.location_wildcards.version,
        )
        (
            nodes.precursor_clusters_old_format,
            nodes.additional_precursor_cluster_stats,
        ) = rules.extract_tables_from_hdf(clusters_hdf=nodes.precursor_clusters_hdf)

        nodes.tims_precursors_reformat_config = (
            rules.get_config_from_db_into_file_system(
                config=configs.tims_reformat_config
            )
        )
        (
            nodes.precursor_clusters,
            nodes.additional_precursor_cluster_stats,
        ) = rules.tims_reformat(
            clusters_startrek=nodes.precursor_clusters_old_format,
            additional_cluster_stats=nodes.additional_precursor_cluster_stats,
            analysis_tdf=nodes.dataset_analysis_tdf,
            config=nodes.tims_precursors_reformat_config,
        )
        # TODO: add optional sorting

    if configs.fragment_clusterer.location_wildcards.software == "tims":
        nodes.tims_fragment_clusterer_config = (
            nodes.tims_precursor_clusterer_config
        ) = rules.get_config_from_db_into_file_system(config=configs.fragment_clusterer)

        (
            nodes.fragment_clusters_hdf,
            nodes.fragment_clustering_stdout,
            nodes.fragment_clustering_stderr,
        ) = rules.cluster_with_tims(
            dataset=nodes.dataset,
            config=nodes.tims_fragment_clusterer_config,
            level=snakemaketools.rules.Wildcard(
                name="level", value="fragment"
            ),  # passing a not-user-defined wildcard
            version=configs.fragment_clusterer.location_wildcards.version,
        )
        (
            nodes.fragment_clusters_old_format,
            nodes.additional_fragment_cluster_stats_old_format,
        ) = rules.extract_tables_from_hdf(clusters_hdf=nodes.fragment_clusters_hdf)

        nodes.tims_fragments_reformat_config = (
            rules.get_config_from_db_into_file_system(
                config=configs.tims_reformat_config
            )
        )

        (
            nodes.fragment_clusters,
            nodes.additional_fragment_cluster_stats,
        ) = rules.tims_reformat(
            clusters_startrek=nodes.fragment_clusters_old_format,
            additional_cluster_stats=nodes.additional_fragment_cluster_stats_old_format,
            analysis_tdf=nodes.dataset_analysis_tdf,
            config=nodes.tims_fragments_reformat_config,
        )
        # TODO: add optional sorting

    # TODO: optimization: instead of .parquet, use .startrek
    nodes.precursor_cluster_stats = rules.get_cluster_stats(
        clusters_startrek=nodes.precursor_clusters
    )
    # TODO: optimization: instead of .parquet, use .startrek
    nodes.fragment_cluster_stats = rules.get_cluster_stats(
        clusters_startrek=nodes.fragment_clusters
    )

    nodes.precursor_prediction_config = rules.get_config_from_db_into_file_system(
        config=configs.precursor_prediction_config
    )
    # TODO: what to do without calibration? Likely need to describe that param as optional
    nodes.fragment_cluster_stats = rules.predict_precursors(
        fragment_cluster_stats=nodes.fragment_cluster_stats,
        fragment_clusters=nodes.fragment_clusters,
        calibration_results=nodes.calibration_results,
        analysis_tdf=nodes.dataset_analysis_tdf,
        config=nodes.precursor_prediction_config,
    )

    if "additional_precursor_cluster_stats" in nodes:
        # TODO: figure out how to pass in variadic number of files with and without names.
        nodes.precursor_cluster_stats = rules.combine_cluster_stats(
            table_0=nodes.precursor_cluster_stats,
            table_1=nodes.additional_precursor_cluster_stats,
        )

    if "additional_fragment_cluster_stats" in nodes:
        # TODO: figure out how to pass in variadic number of files with and without names.
        nodes.fragment_cluster_stats = rules.combine_cluster_stats(
            table_0=nodes.fragment_cluster_stats,
            table_1=nodes.additional_fragment_cluster_stats,
        )

    nodes.matching_config = rules.get_config_from_db_into_file_system(
        config=configs.matching
    )
    nodes.rough_matches = rules.roughly_match_precursors_and_fragments(
        precursor_cluster_stats=nodes.precursor_cluster_stats,
        fragment_cluster_stats=nodes.fragment_cluster_stats,
        config=nodes.matching_config,
    )

    nodes.mgf_config = rules.get_config_from_db_into_file_system(
        config=configs.mgf_config
    )
    nodes.rough_mgf = rules.write_mgf(
        precursor_cluster_stats=nodes.precursor_cluster_stats,
        fragment_cluster_stats=nodes.fragment_cluster_stats,
        matches=nodes.rough_matches,
        config=nodes.mgf_config,
    )

    if "sage_config" in configs:
        nodes.sage_config = rules.get_config_from_db_into_file_system(
            config=configs.sage_config
        )
        nodes.sage_exe = rules.get_sage(
            version=configs.sage_config.location_wildcards.version
            # TODO: consider putting all wildcards in one place?
            # wildcards.sage.version would be shorter, thus more intuitive.
        )
        (
            nodes.first_gen_sage_results,
            nodes.first_gen_sage_results_json,
            nodes.first_gen_search_precurors,
            nodes.first_gen_sage_result_sage_tsv,
            nodes.first_gen_search_fragments,
            nodes.first_gen_search_results_sage_pin,
            nodes.first_gen_sage_sage_stderr,
            nodes.first_gen_sage_sage_stdout,
        ) = rules.search_with_SAGE(
            mgf=nodes.rough_mgf,
            fasta=nodes.fasta,
            config=nodes.sage_config,
            version=configs.sage_config.location_wildcards.version,
            sage=nodes.sage_exe,
        )

        nodes.map_back_sage_results_unto_peptide_fragment_graph_config = (
            rules.get_config_from_db_into_file_system(
                config=configs.map_back_sage_results_unto_peptide_fragment_graph_config
            )
        )

        (
            nodes.first_gen_fdr_filtered_precursors,
            nodes.first_gen_fdr_filtered_fragments,
            nodes.first_gen_fdr_filtered_edges,
            nodes.first_gen_quality_control_folder,
        ) = rules.map_back_sage_results_unto_peptide_fragment_graph(
            found_precursors=nodes.first_gen_search_precurors,
            found_fragments=nodes.first_gen_search_fragments,
            fragment_cluster_stats=nodes.fragment_cluster_stats,
            matches=nodes.rough_matches,
            config=nodes.map_back_sage_results_unto_peptide_fragment_graph_config,
        )

    nodes.node_refinement_config = rules.get_config_from_db_into_file_system(
        config=configs.node_refinement_config
    )

    (
        nodes.refined_precursor_stats,
        nodes.refined_fragment_stats,
        nodes.mz_recalibrated_distributions,
        nodes.refined_nodes_quality_checks,
    ) = rules.refine_nodes(
        filtered_precursors=nodes.first_gen_fdr_filtered_precursors,
        filtered_matches=nodes.first_gen_fdr_filtered_edges,
        uncalibrated_precursor_stats=nodes.precursor_cluster_stats,
        uncalibrated_fragment_stats=nodes.fragment_cluster_stats,
        config=nodes.node_refinement_config,
    )

    nodes.second_gen_sage_config = nodes.sage_config
    if "sage_search_update_config" in configs:
        nodes.sage_search_update_config = rules.get_config_from_db_into_file_system(
            config=configs.sage_search_update_config
        )
        nodes.second_gen_sage_config = rules.refine_sage_config(
            sage_config=nodes.sage_config,
            mz_recalibrated_distributions=nodes.mz_recalibrated_distributions,
            config=nodes.sage_search_update_config,
        )

    nodes.edge_refinement_config = rules.get_config_from_db_into_file_system(
        config=configs.edge_refinement_config
    )

    (
        nodes.refined_matches,
        nodes.refined_matches_stats,
        nodes.remaining_first_gen_edges_counts,
        nodes.refined_matches_qc,
    ) = rules.refine_edges(
        precursor_stats=nodes.refined_precursor_stats,
        fragment_stats=nodes.refined_fragment_stats,
        all_edges=nodes.rough_matches,
        filtered_edges=nodes.first_gen_fdr_filtered_edges,
        hard_filtered_edges=nodes.first_gen_fdr_filtered_edges,  # TODO: this needs some NULL or equivalent.
        config=nodes.edge_refinement_config,
    )

    nodes.second_gen_mgf = rules.write_mgf(
        precursor_cluster_stats=nodes.refined_precursor_stats,
        fragment_cluster_stats=nodes.refined_fragment_stats,
        matches=nodes.refined_matches,
        config=nodes.mgf_config,
    )

    if "second_gen_sage_config" in configs:
        (
            nodes.second_gen_sage_results,
            nodes.second_gen_sage_results_json,
            nodes.second_gen_search_precurors,
            nodes.second_gen_sage_result_sage_tsv,
            nodes.second_gen_search_fragments,
            nodes.second_gen_search_results_sage_pin,
            nodes.second_gen_sage_sage_stderr,
            nodes.second_gen_sage_sage_stdout,
        ) = rules.search_with_SAGE(
            mgf=nodes.second_gen_mgf,
            fasta=nodes.fasta,
            config=nodes.second_gen_sage_config,
            version=configs.sage_config.location_wildcards.version,
            sage=nodes.sage_exe,
        )
        (
            nodes.second_gen_fdr_filtered_precursors,
            nodes.second_gen_fdr_filtered_fragments,
            nodes.second_gen_fdr_filtered_edges,
            nodes.second_gen_quality_control_folder,
        ) = rules.map_back_sage_results_unto_peptide_fragment_graph(
            found_precursors=nodes.second_gen_search_precurors,
            found_fragments=nodes.second_gen_search_fragments,
            fragment_cluster_stats=nodes.refined_fragment_stats,
            matches=nodes.refined_matches,
            config=nodes.map_back_sage_results_unto_peptide_fragment_graph_config,
        )

    # rules.run_compomics_rescoring(sage_results_tsv = , mgf = , fasta = , config = , search_config = )

    return nodes

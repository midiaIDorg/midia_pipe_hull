"""
I guess we can assume that pipelines will be simply importable functions.

snakemaketools: general long snake capabilities.
"""
import snakemaketools.rules

from snakemaketools.datastructures import DotDict


# TODO: move sagepy out of here into a separate pipeline.
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
    nodes.fasta_stats = rules.summarize_fasta(fasta=nodes.fasta)

    nodes.dataset = rules.fetch_data(folder_d=wildcards.dataset)
    nodes.dataset_hashes = rules.hash_d(folder=nodes.dataset)

    nodes.dataset_marginal_distribution_plots = rules.get_marginal_distribution_plots(
        raw_data=nodes.dataset
    )
    nodes.memmapped_dataset = rules.memmap_data(folder_d=nodes.dataset)

    nodes.raw_data_2D_histograms = rules.raw_data_2D_histograms(
        dataset=nodes.dataset, memmapped_dataset=nodes.memmapped_dataset
    )

    # nodes.calibration_results = snakemaketools.rules.Node(location="None")
    nodes.calibration = rules.fetch_data(folder_d=wildcards.calibration)

    nodes.calibration_hashes = rules.hash_d(folder=nodes.calibration)

    nodes.calibration_results = snakemaketools.rules.Node(location="none")
    nodes.dataset_matches_calibration_assertion = snakemaketools.rules.Node(
        location="none"
    )

    if not "None.d" in nodes.calibration.location:
        nodes.dataset_matches_calibration_assertion = (
            rules.report_if_dataset_and_calibration_comply(
                dataset=nodes.dataset,
                calibration=nodes.calibration,
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

    nodes.tims_precursor_clusterer_config = rules.get_config_from_db_into_file_system(
        config=configs.precursor_clusterer
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

    # TODO: These steps are needed only for compatibility with the old pipeline..
    # but as it is nothing really done everytime: could simply do it above.
    nodes.tims_precursors_reformat_config = rules.get_config_from_db_into_file_system(
        config=configs.tims_reformat_config
    )
    (
        nodes.precursor_clusters,
        nodes.additional_precursor_cluster_stats,
    ) = rules.tims_reformat(
        clusters_startrek=nodes.precursor_clusters_old_format,
        additional_cluster_stats=nodes.additional_precursor_cluster_stats,
        dataset=nodes.dataset,
        config=nodes.tims_precursors_reformat_config,
    )
    # TODO: add optional sorting

    # TODO: optimization: instead of .parquet, use .startrek
    nodes.precursor_cluster_stats = rules.get_cluster_stats(
        clusters_startrek=nodes.precursor_clusters
    )

    nodes.tims_fragment_clusterer_config = rules.get_config_from_db_into_file_system(
        config=configs.fragment_clusterer
    )

    nodes.tims_thprs_folder = rules.cluster_with_tims_on_thprs(
        dataset=nodes.dataset,
        config=nodes.tims_fragment_clusterer_config,
        precursor_stats=nodes.precursor_cluster_stats,
        version=configs.fragment_clusterer.location_wildcards.version,
    )

    nodes.summarize_thprs_config = rules.get_config_from_db_into_file_system(
        config=configs.thprs_summary
    )

    (
        nodes.fragment_clusters_old_format,
        nodes.additional_fragment_cluster_stats_old_format,
        nodes.fragment_clustering_stdout,
        nodes.fragment_clustering_stderr,
    ) = rules.summarize_thprs(
        tims_thprs_folder=nodes.tims_thprs_folder,
        config=nodes.summarize_thprs_config,
    )

    # These steps are needed only for compatibility with the old pipeline..
    # but as it is nothing really done everytime: could simply do it above.
    nodes.tims_fragments_reformat_config = rules.get_config_from_db_into_file_system(
        config=configs.tims_reformat_config
    )
    (
        nodes.fragment_clusters,
        nodes.additional_fragment_cluster_stats,
    ) = rules.tims_reformat(
        clusters_startrek=nodes.fragment_clusters_old_format,
        additional_cluster_stats=nodes.additional_fragment_cluster_stats_old_format,
        dataset=nodes.dataset,
        config=nodes.tims_fragments_reformat_config,
    )

    # TODO: optimization: instead of .parquet, use .startrek
    nodes.fragment_cluster_stats = rules.get_cluster_stats(
        clusters_startrek=nodes.fragment_clusters
    )

    # predicting precursors
    nodes.precursor_prediction_config = rules.get_config_from_db_into_file_system(
        config=configs.precursor_prediction_config
    )

    # TODO: why predict_precursors needs all this???
    # TODO: what to do without calibration? Likely need to describe that param as optional or set to None.
    nodes.fragment_cluster_stats = rules.predict_precursors(
        fragment_cluster_stats=nodes.fragment_cluster_stats,
        fragment_clusters=nodes.fragment_clusters,
        calibration_results=nodes.calibration_results,
        dataset=nodes.dataset,
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

    first_gen_sage_config = configs.get("first_gen_sage_config", configs.get("sage_config"))
    nodes.first_gen_sage_config = rules.get_config_from_db_into_file_system(
        config=first_gen_sage_config
    )

    nodes.first_gen_sage_exe = rules.get_sage(
        version=first_gen_sage_config.location_wildcards.version
    )
    # TODO: consider putting all wildcards in one place?
    # wildcards.sage.version would be shorter, thus more intuitive.

    (
        nodes.first_gen_sage_results_json,
        nodes.first_gen_search_precursors,
        nodes.first_gen_sage_result_sage_tsv,
        nodes.first_gen_search_fragments,  # imprecise: more like edges.
        nodes.first_gen_search_results_sage_pin,
        nodes.first_gen_sage_sage_stderr,
        nodes.first_gen_sage_sage_stdout,
    ) = rules.search_with_SAGE(
        mgf=nodes.rough_mgf,
        fasta=nodes.fasta,
        config=nodes.first_gen_sage_config,
        version=first_gen_sage_config.location_wildcards.version,
        sage=nodes.first_gen_sage_exe,
    )

    nodes.first_gen_fdr_filter_config = rules.get_config_from_db_into_file_system(
        config=configs.first_gen_fdr_filter_config
    )

    (
        nodes.first_gen_fdr_filtered_precursors,
        nodes.first_gen_fdr_filtered_fragments,
    ) = rules.apply_fdr_filter_to_sage_results(
        config=nodes.first_gen_fdr_filter_config,
        found_precursors=nodes.first_gen_search_precursors,
        found_fragments=nodes.first_gen_search_fragments,
    )

    nodes.filtered_search_summary_config = rules.get_config_from_db_into_file_system(
        config=configs.filtered_search_summary_config
    )

    nodes.first_gen_fdr_filtered_search_stats = rules.stat_sage_results(
        config=nodes.filtered_search_summary_config,
        precursors=nodes.first_gen_fdr_filtered_precursors,
        fragments=nodes.first_gen_fdr_filtered_fragments,
    )

    nodes.map_back_sage_results_unto_peptide_fragment_graph_config = (
        rules.get_config_from_db_into_file_system(
            config=configs.map_back_sage_results_unto_peptide_fragment_graph_config
        )
    )

    (
        nodes.first_gen_fdr_filtered_mapped_back_precursors,
        nodes.first_gen_fdr_filtered_mapped_back_fragments,  # not used for anything???
        nodes.first_gen_fdr_filtered_mapped_back_edges,
        nodes.first_gen_quality_control_folder,
    ) = rules.map_back_sage_results_unto_peptide_fragment_graph(
        fdf_filtered_precursors=nodes.first_gen_fdr_filtered_precursors,
        fdf_filtered_fragments=nodes.first_gen_fdr_filtered_fragments,
        fragment_cluster_stats=nodes.fragment_cluster_stats,
        matches=nodes.rough_matches,
        config=nodes.map_back_sage_results_unto_peptide_fragment_graph_config,
    )

    nodes.filtered_mapped_back_search_summary_config = (
        rules.get_config_from_db_into_file_system(
            config=configs.filtered_mapped_back_search_summary_config
        )
    )

    nodes.first_gen_fdr_filtered_mapped_back_search_stats = rules.stat_sage_results(
        config=nodes.filtered_mapped_back_search_summary_config,
        precursors=nodes.first_gen_fdr_filtered_mapped_back_precursors,
        fragments=nodes.first_gen_fdr_filtered_mapped_back_fragments,
    )

    nodes.first_gen_fdr_filtered_mapped_back_search_results_plot = rules.overplot_sage_results_on_window_groups(
        precursor_stats_path=nodes.precursor_cluster_stats,
        filtered_mapped_back_sage_results_path=nodes.first_gen_fdr_filtered_mapped_back_precursors,
        dataset_path=nodes.dataset,
        rawdata_histograms_path=nodes.raw_data_2D_histograms,
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
        filtered_precursors=nodes.first_gen_fdr_filtered_mapped_back_precursors,
        filtered_matches=nodes.first_gen_fdr_filtered_mapped_back_edges,
        uncalibrated_precursor_stats=nodes.precursor_cluster_stats,
        uncalibrated_fragment_stats=nodes.fragment_cluster_stats,
        config=nodes.node_refinement_config,
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
        filtered_edges=nodes.first_gen_fdr_filtered_mapped_back_edges,
        hard_filtered_edges=nodes.first_gen_fdr_filtered_mapped_back_edges,  # TODO: this needs some NULL or equivalent.
        config=nodes.edge_refinement_config,
    )

    nodes.second_gen_mgf = rules.write_mgf(
        precursor_cluster_stats=nodes.refined_precursor_stats,
        fragment_cluster_stats=nodes.refined_fragment_stats,
        matches=nodes.refined_matches,
        config=nodes.mgf_config,
    )

    if "peaks_mgf_config" in configs:
        nodes.peaks_mgf_config = rules.get_config_from_db_into_file_system(
            config=configs.peaks_mgf_config
        )
        nodes.second_gen_mgf_peaks = rules.write_mgf(
            precursor_cluster_stats=nodes.refined_precursor_stats,
            fragment_cluster_stats=nodes.refined_fragment_stats,
            matches=nodes.refined_matches,
            config=nodes.peaks_mgf_config,
        )

    second_gen_sage_config = configs.get(
        "second_gen_sage_config", configs.get("sage_config")
    )
    nodes.second_gen_sage_config = rules.get_config_from_db_into_file_system(
        config=second_gen_sage_config
    )

    if "sage_search_update_config" in configs:
        nodes.sage_search_update_config = rules.get_config_from_db_into_file_system(
            config=configs.sage_search_update_config
        )
        nodes.second_gen_sage_config = rules.refine_sage_config(
            sage_config=nodes.second_gen_sage_config,
            mz_recalibrated_distributions=nodes.mz_recalibrated_distributions,
            config=nodes.sage_search_update_config,
        )

    nodes.second_gen_sage_exe = rules.get_sage(
        version=second_gen_sage_config.location_wildcards.version
    )

    (
        nodes.second_gen_sage_results_json,
        nodes.second_gen_search_precursors,
        nodes.second_gen_sage_result_sage_tsv,
        nodes.second_gen_search_fragments,
        nodes.second_gen_search_results_sage_pin,
        nodes.second_gen_sage_sage_stderr,
        nodes.second_gen_sage_sage_stdout,
    ) = rules.search_with_SAGE(
        mgf=nodes.second_gen_mgf,
        fasta=nodes.fasta,
        config=nodes.second_gen_sage_config,
        version=second_gen_sage_config.location_wildcards.version,
        sage=nodes.second_gen_sage_exe,
    )

    # TODO: add some config.
    nodes.second_gen_fdr_filter_config = rules.get_config_from_db_into_file_system(
        config=configs.second_gen_fdr_filter_config
    )

    (  # TODO: need to stat both this and after mapping back
        nodes.second_gen_fdr_filtered_precursors,
        nodes.second_gen_fdr_filtered_fragments,
    ) = rules.apply_fdr_filter_to_sage_results(
        config=nodes.second_gen_fdr_filter_config,
        found_precursors=nodes.second_gen_search_precursors,
        found_fragments=nodes.second_gen_search_fragments,
    )

    nodes.second_gen_fdr_filtered_search_stats = rules.stat_sage_results(
        config=nodes.filtered_search_summary_config,
        precursors=nodes.second_gen_fdr_filtered_precursors,
        fragments=nodes.second_gen_fdr_filtered_fragments,
    )
    # rules.run_compomics_rescoring(sage_results_tsv = , mgf = , fasta = , config = , search_config = )

    node_names_with_tables_to_summarize: list[str] = [
        "precursor_cluster_stats",
        "fragment_cluster_stats",
        "rough_matches",
        "first_gen_search_precursors",
        "first_gen_search_fragments",
        "refined_precursor_stats",
        "refined_fragment_stats",
        "mz_recalibrated_distributions",
        "refined_matches",
    ]
    for node_name in node_names_with_tables_to_summarize:
        nodes[f"{node_name}_summary"] = rules.summarize_table(table=nodes[node_name])

    (
        nodes.second_gen_fdr_filtered_mapped_back_precursors,
        nodes.second_gen_fdr_filtered_mapped_back_fragments,  # not used for anything???
        nodes.second_gen_fdr_filtered_mapped_back_edges,
        nodes.second_gen_quality_control_folder,
    ) = rules.map_back_sage_results_unto_peptide_fragment_graph(
        fdf_filtered_precursors=nodes.second_gen_fdr_filtered_precursors,
        fdf_filtered_fragments=nodes.second_gen_fdr_filtered_fragments,
        fragment_cluster_stats=nodes.refined_fragment_stats,
        matches=nodes.rough_matches,
        config=nodes.map_back_sage_results_unto_peptide_fragment_graph_config,
    )

    nodes.second_gen_fdr_filtered_mapped_back_search_stats = rules.stat_sage_results(
        config=nodes.filtered_mapped_back_search_summary_config,
        precursors=nodes.second_gen_fdr_filtered_mapped_back_precursors,
        fragments=nodes.second_gen_fdr_filtered_mapped_back_fragments,
    )

    nodes.second_gen_fdr_filtered_mapped_back_search_results_plot = rules.overplot_sage_results_on_window_groups(
        precursor_stats_path=nodes.precursor_cluster_stats,
        filtered_mapped_back_sage_results_path=nodes.second_gen_fdr_filtered_mapped_back_precursors,
        dataset_path=nodes.dataset,
        rawdata_histograms_path=nodes.raw_data_2D_histograms,
    )

    return nodes

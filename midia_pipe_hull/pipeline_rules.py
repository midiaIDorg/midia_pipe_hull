"""
This module contains the functional equivalents of Snakemake rules.

Notice that they follow a template.
Hence: in the future those functions will be dynamically created at runtime, either from snakemake rules or simply scripts used by those rules.
"""

from functools import partial

from snakemaketools.models import Path, RuleOrConfig

# for subconfig_type, subconfig in subconfigs.items():


def assert_subconfig_is_valid(subconfig_type: str, subconfig: dict) -> None:
    assert (
        "_config" == subconfig_type[-len("_config") :]
    ), "Subconfig type must end in `_config`"
    assert (
        "extension" in subconfig
    ), f"Each subconfig must specify its extension, e.g. `.toml` or `.json` or `.config`.\nBut `{subconfig_type}` did not."


def get_subconfig(subconfig_type: str, subconfig: dict) -> Path:
    config = RuleOrConfig.GETINSERT(
        meta={"subconfig": subconfig, "inputs": {}},
        type=subconfig_type,
    )
    return Path.GETINSERT(
        path=f"tmp/configs/{subconfig_type}/{config.id}{subconfig['extension']}",
        type=subconfig_type,
        rule_or_config=config,
    )


def register_tdf_rawdata(rawdata_tdf: str) -> tuple[Path, Path, Path]:
    rule = RuleOrConfig.GETINSERT(
        meta=dict(rawdata_tdf=rawdata_tdf, inputs={}),
        type="register_tdf_rawdata",
    )
    folder_d = Path.GETINSERT(
        path=f"spectra/{rawdata_tdf}.d",
        type="raw_data",
        rule_or_config=rule,
    )
    analysis_tdf = Path.GETINSERT(
        path=f"{folder_d.path}/analysis.tdf",
        type="sqlite",
        rule_or_config=rule,
    )
    analysis_tdf_bin = Path.GETINSERT(
        path=f"{folder_d.path}/analysis.tdf_bin",
        type="tdf_bin",
        rule_or_config=rule,
    )
    return folder_d, analysis_tdf, analysis_tdf_bin


def register_fasta(fasta: str) -> Path:
    rule = RuleOrConfig.GETINSERT(
        meta=dict(fasta=fasta, inputs={}),
        type="register_fasta",
    )
    fasta = Path.GETINSERT(
        path=f"fastas/{fasta}.fasta",
        type="fasta",
        rule_or_config=rule,
    )
    return fasta


def hash256(path: Path) -> Path:
    assert path.id is not None
    rule = RuleOrConfig.GETINSERT(
        meta=dict(inputs={"path": path.id}),
        type="hash256",
    )
    hashfile = Path.GETINSERT(
        path=f"tmp/hashes/{rule.id}.hash256",
        type="hash256",
        rule_or_config=rule,
    )
    return hashfile


def remove_rawdata_baseline(
    raw_data: Path,
    config: Path,
) -> tuple[Path, Path, Path]:
    assert raw_data.id != None
    assert raw_data.type == "raw_data"
    assert config.id != None
    assert config.type == "baseline_removal_config"

    rule = RuleOrConfig.GETINSERT(
        meta=dict(
            inputs={
                "raw_data": raw_data.id,
                "config": config.id,
            }
        ),
        type="remove_rawdata_baseline",
    )
    folder_d = Path.GETINSERT(
        path=f"tmp/spectra/no_baseline/{rule.id}.d",
        type="raw_data",
        rule_or_config=rule,
    )
    analysis_tdf = Path.GETINSERT(
        path=f"{folder_d.path}/analysis.tdf",
        type="sqlite",
        rule_or_config=rule,
    )
    analysis_tdf_bin = Path.GETINSERT(
        path=f"{folder_d.path}/analysis.tdf_bin",
        type="tdf_bin",
        rule_or_config=rule,
    )
    return folder_d, analysis_tdf, analysis_tdf_bin


def report_if_dataset_and_calibration_comply(
    dataset: Path,
    calibration: Path,
) -> Path:
    rule = RuleOrConfig.GETINSERT(
        meta=dict(
            inputs={
                "dataset": dataset.id,
                "calibration": calibration.id,
            }
        ),
        type="dataset_matches_calibration_assertion",
    )
    assertion = Path.GETINSERT(
        path=f"tmp/assertions/dataset_matches_calibration/{rule.id}.d",
        type="dataset_matches_calibration_assertion",
        rule_or_config=rule,
    )
    return assertion


def raw_data_marginals_plots_folder(raw_data: Path) -> Path:
    assert raw_data.id != None
    assert raw_data.type == "raw_data"

    rule = RuleOrConfig.GETINSERT(
        meta=dict(
            inputs={
                "raw_data": raw_data.id,
            }
        ),
        type="raw_data_marginals_plots_folder",
    )
    plots_of_marginal_distributions = Path.GETINSERT(
        path=f"tmp/raw_data_marginals/{rule.id}.d",
        type="raw_data_marginals_plots_folder",
        rule_or_config=rule,
    )
    return plots_of_marginal_distributions


def cluster_with_tims(
    dataset: Path, config: Path, level: str
) -> tuple[Path, Path, Path]:
    assert dataset.id != None
    assert dataset.type == "raw_data"
    assert config.id != None
    assert config.type == f"{level}_clustering_config"

    rule = RuleOrConfig.GETINSERT(
        meta=dict(
            inputs={
                "dataset": dataset.id,
            }
        ),
        type=f"cluster_{level}s_with_tims",
    )
    precursor_clusters_hdf = Path.GETINSERT(
        path=f"tmp/{level}_clusters/tims/{rule.id}.hdf",
        type=f"{level}_clusters_hdf",
        rule_or_config=rule,
    )
    precursor_clustering_stdout = Path.GETINSERT(
        path=f"tmp/{level}_clusters/tims/{rule.id}.stdout",
        type=f"tims_{level}_clusters_stdout",
        rule_or_config=rule,
    )
    precursor_clustering_stderr = Path.GETINSERT(
        path=f"tmp/{level}_clusters/tims/{rule.id}.stderr",
        type=f"tims_{level}_clusters_stderr",
        rule_or_config=rule,
    )

    return (
        precursor_clusters_hdf,
        precursor_clustering_stdout,
        precursor_clustering_stderr,
    )


cluster_precursors_with_tims = partial(cluster_with_tims, level="precursor")
cluster_fragments_with_tims = partial(cluster_with_tims, level="fragment")


def postprocess_tims_clusters(
    clusters_hdf: Path,
) -> tuple[Path, Path]:
    return clusters, additional_cluster_stats


def postprocess_precursor_tims_clusters(
    precursor_clusters_hdf: Path,
) -> tuple[Path, Path]:
    return precursor_clusters, additional_precursor_cluster_stats


def postprocess_fragments_tims_clusters(
    fragment_clusters_hdf: Path,
) -> tuple[Path, Path]:
    return fragment_clusters, additional_fragment_cluster_stats


# def cluster(
#     raw_data: Path,
#     config: Path,
#     type: str,
#     rule: str,
# ) -> tuple[Path, Path, Path]:
#     for arg in (raw_data, config):
#         assert arg.id is not None

#     rule = RuleOrConfig.GETINSERT(
#         meta=dict(
#             inputs={
#                 "raw_data": raw_data.id,
#                 "config": config.id,
#             }
#         ),
#         type=type,
#     )

#     data = Path.GETINSERT(
#         path=
#         origin=_origin, type=type, extension=".startrek")
#     stdout = Path.GETINSERT(origin=_origin, type="stdout", extension=".txt")
#     stderr = Path.GETINSERT(origin=_origin, type="stderr", extension=".txt")

#     return data, stdout, stderr


# cluster_precursors = partial(cluster, type="precursor_clusters")
# cluster_fragments = partial(cluster, type="fragment_clusters")


# def get_cluster_stats(clusters: Path, config: Path) -> Path:
#     for arg in (clusters, config):
#         assert arg.id is not None

#     _origin = dict(
#         rule="get_cluster_stats",
#         inputs=dict(clusters=clusters.id, config=config.id),
#     )

#     cluster_stats = Path.GETINSERT(origin=_origin, type="cluster_stats")
#     return cluster_stats


# def match_precursors_and_fragments(
#     precursor_stats: Path, fragment_stats: Path, matching_config: Path
# ) -> Path:
#     for arg in (precursor_stats, fragment_stats, matching_config):
#         assert arg.id is not None

#     _origin = dict(
#         rule="match_precursors_and_fragments",
#         inputs=dict(
#             precursor_stats=precursor_stats.id,
#             fragment_stats=fragment_stats.id,
#             matching_config=matching_config.id,
#         ),
#     )

#     rough_matches = Path.GETINSERT(origin=_origin, type="rough_matches.startrek")

#     return rough_matches

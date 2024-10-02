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


# why do I need to keep the ids? It will be more useful to directly keep paths.


def get_subconfig(subconfig_type: str, subconfig: dict, inputs: dict = {}) -> Path:
    config = RuleOrConfig.GETINSERT(
        meta={"subconfig": subconfig, "inputs": inputs},
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
        type="analysis_tdf",
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
        path=f"tmp/hashes/{rule.id}.sha256",
        type="hash256",
        rule_or_config=rule,
    )
    return hashfile


def remove_raw_data_baseline(
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
                "dataset": raw_data.id,
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
        type="analysis_tdf",
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


# TODO: test this.
def get_tims_executable(subconfig: dict) -> Path:
    exe = f"software/{subconfig['software']}/{subconfig['version']}/{subconfig['executable']}"
    rule = RuleOrConfig.GETINSERT(
        meta=dict(inputs={"executable": exe}),
        type=f"get_tims_executable",
    )
    tims_executable = Path.GETINSERT(
        path=exe,
        type=f"tims_executable",
        rule_or_config=rule,
    )
    return tims_executable


def cluster_with_tims(
    dataset: Path,
    config: Path,
    executable: Path,
    level: str,
) -> tuple[Path, Path]:
    assert dataset.id != None
    assert dataset.type == "raw_data"
    assert config.id != None
    assert config.type == f"{level}_clustering_config"

    rule = RuleOrConfig.GETINSERT(
        meta=dict(
            inputs={
                "dataset": dataset.id,
                "config": config.id,
                "executable": executable.id,
            },
            level="level",
        ),
        type=f"cluster_{level}s_with_tims",
    )
    clusters_hdf = Path.GETINSERT(
        path=f"tmp/{level}_clusters/tims/{rule.id}/precursors.hdf",
        type=f"{level}_clusters_hdf",
        rule_or_config=rule,
    )
    clusters_qc = Path.GETINSERT(
        path=f"tmp/{level}_clusters/tims/{rule.id}/qc",
        type=f"tims_{level}_clusters_qc",
        rule_or_config=rule,
    )

    return clusters_hdf, clusters_qc


cluster_precursors_with_tims = partial(
    cluster_with_tims,
    level="precursor",
)
cluster_fragments_with_tims = partial(
    cluster_with_tims,
    level="fragment",
)


def postprocess_tims_clusters(
    clusters_hdf: Path,
    analysis_tdf: Path,
    level: str,
) -> tuple[Path, Path]:
    assert clusters_hdf.id != None
    assert "clusters_hdf" in clusters_hdf.type
    assert analysis_tdf.id != None
    assert analysis_tdf.type == "analysis_tdf"
    rule = RuleOrConfig.GETINSERT(
        meta=dict(
            inputs={"clusters_hdf": clusters_hdf.id, "analysis_tdf": analysis_tdf.id}
        ),
        type=f"postprocess_tims_{level}s_clusters",
    )
    postprocessed_clusters_hdf = Path.GETINSERT(
        path=f"tmp/tims/postprocessed_{level}_tims_clusters/{rule.id}.startrek",
        type=f"postprocessed_{level}_tims_clusters",
        rule_or_config=rule,
    )
    additional_cluster_stats = Path.GETINSERT(
        path=f"tmp/tims/postprocessed_{level}_tims_clusters/{rule.id}.parquet",
        type=f"postprocessed_tims_additional_{level}_clusters_stats",
        rule_or_config=rule,
    )
    return postprocessed_clusters_hdf, additional_cluster_stats


postprocess_precursor_tims_clusters = partial(
    postprocess_tims_clusters,
    level="precursor",
)
postprocess_fragment_tims_clusters = partial(
    postprocess_tims_clusters,
    level="fragment",
)

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

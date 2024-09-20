"""
This module contains the functional equivalents of Snakemake rules.

Notice that they follow a template.
Hence: in the future those functions will be dynamically created at runtime, either from snakemake rules or simply scripts used by those rules.
"""

from functools import partial

from snakemaketools.models import Path, RuleOrConfig


def register_subconfigs(paths: dict, subconfigs: dict) -> None:
    for subconfig_type, subconfig in subconfigs.items():
        assert (
            "_config" == subconfig_type[-len("_config") :]
        ), "Subconfig type must end in `_config`"
        assert (
            "extension" in subconfig
        ), f"Each subconfig must specify its extension, e.g. `.toml` or `.json` or `.config`.\nBut `{subconfig_type}` did not."
    for subconfig_type, subconfig in subconfigs.items():
        config = RuleOrConfig.GETINSERT(
            meta={"subconfig": subconfig, "inputs": {}},
            type=subconfig_type,
        )
        paths[subconfig_type] = Path.GETINSERT(
            path=f"tmp/configs/{subconfig_type}/{config.id}{subconfig['extension']}",
            type=subconfig_type,
            rule_or_config=config,
        )


def register_tdf_rawdata(rawdata_tdf):
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


def register_fasta(fasta):
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


# def cluster(
#     raw_data: Node,
#     config: Node,
#     type: str,
#     rule: str,
# ) -> tuple[Node, Node, Node]:
#     for arg in (raw_data, config):
#         assert arg.id is not None

#     _origin = dict(
#         rule=rule,
#         inputs=dict(raw_data=raw_data.id, config=config.id),
#     )

#     data = Node.GETINSERT(origin=_origin, type=type, extension=".startrek")
#     stdout = Node.GETINSERT(origin=_origin, type="stdout", extension=".txt")
#     stderr = Node.GETINSERT(origin=_origin, type="stderr", extension=".txt")

#     return data, stdout, stderr


# cluster_precursors = partial(cluster, type="precursor_clusters")
# cluster_fragments = partial(cluster, type="fragment_clusters")


# def get_cluster_stats(clusters: Node, config: Node) -> Node:
#     for arg in (clusters, config):
#         assert arg.id is not None

#     _origin = dict(
#         rule="get_cluster_stats",
#         inputs=dict(clusters=clusters.id, config=config.id),
#     )

#     cluster_stats = Node.GETINSERT(origin=_origin, type="cluster_stats")
#     return cluster_stats


# def remove_rawdata_baseline(raw_data: Node, config: Node) -> Node:
#     for arg in (raw_data, config):
#         assert arg.id is not None

#     _origin = dict(
#         rule="remove_rawdata_baseline",
#         inputs=dict(dataset=raw_data.id, config=config.id),
#     )

#     raw_data_without_baseline = Node.GETINSERT(
#         origin=_origin,
#         type="raw_data_without_baseline",
#         extension=".d",
#     )
#     return raw_data_without_baseline


# def match_precursors_and_fragments(
#     precursor_stats: Node, fragment_stats: Node, matching_config: Node
# ) -> Node:
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

#     rough_matches = Node.GETINSERT(origin=_origin, type="rough_matches.startrek")

#     return rough_matches

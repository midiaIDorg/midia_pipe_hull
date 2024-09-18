from snakemaketools.models import Node


def cluster(raw_data: Node, config: Node) -> tuple[Node, Node, Node]:
    for arg in (raw_data, config):
        assert arg.id is not None

    _origin = dict(
        rule="cluster",
        inputs=dict(raw_data=raw_data.id, config=config.id),
    )

    data = Node.GETINSERT(origin=_origin, type="clusters.startrek")
    stdout = Node.GETINSERT(origin=_origin, type="stdoud.txt")
    stderr = Node.GETINSERT(origin=_origin, type="stderr.txt")

    return data, stdout, stderr


cluster_fragments = cluster_precursors = cluster


def get_cluster_stats(clusters: Node, config: Node) -> Node:
    for arg in (clusters, config):
        assert arg.id is not None

    _origin = dict(
        rule="get_cluster_stats",
        inputs=dict(clusters=clusters.id, config=config.id),
    )

    cluster_stats = Node.GETINSERT(origin=_origin, type="cluster_stats.parquet")
    return cluster_stats


def remove_rawdata_baseline(raw_data: Node, config: Node) -> Node:
    for arg in (raw_data, config):
        assert arg.id is not None

    _origin = dict(
        rule="remove_rawdata_baseline",
        inputs=dict(raw_data=raw_data.id, config=config.id),
    )

    raw_data_without_baseline = Node.GETINSERT(origin=_origin, type="tdf.d")
    return raw_data_without_baseline


def match_precursors_and_fragments(
    precursor_stats: Node, fragment_stats: Node, matching_config: Node
) -> Node:
    for arg in (precursor_stats, fragment_stats, matching_config):
        assert arg.id is not None

    _origin = dict(
        rule="match_precursors_and_fragments",
        inputs=dict(
            precursor_stats=precursor_stats.id,
            fragment_stats=fragment_stats.id,
            matching_config=matching_config.id,
        ),
    )

    rough_matches = Node.GETINSERT(origin=_origin, type="rough_matches.startrek")

    return rough_matches

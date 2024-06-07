#!/usr/bin/env python3

import argparse
from pathlib import Path
from pprint import pprint
from types import SimpleNamespace

from numba_progress import ProgressBar
from tqdm import tqdm

import duckdb
import matplotlib.pyplot as plt
import numba
import numpy as np
import numpy.typing as npt
import tomllib
from mmapped_df import GroupedIndex
from pandas_ops.io import read_df

_development = False
if _development:
    folder = Path("out/base/debug_mgf/default_wildcards")
    args = SimpleNamespace(
        precursor_stats=Path(folder / "precursor_stats.parquet"),
        fragment_stats=Path(folder / "fragment_stats.parquet"),
        edges=Path(folder / "rough_matches.startrek"),
        config=Path("configs/mgf/sagemgf.toml"),
        out=Path("/home/matteo/test.mgf"),
    )
else:
    parser = argparse.ArgumentParser(
        "Represent the MIDIA graph in form of an MGF file. Mapping between the two should be bijective: don't add any filtering steps here, but upstream or downstream.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "precursor_stats",
        help="Path to the statistics on precursor clusters.",
        type=Path,
    )
    parser.add_argument(
        "fragment_stats",
        help="Path to the statistics on fragments clusters.",
        type=Path,
    )
    parser.add_argument(
        "edges",
        help="Path to connections between precursors and fragments.",
        type=Path,
    )
    parser.add_argument(
        "config",
        help="Path to the config file.",
        type=Path,
    )
    parser.add_argument("mgf_path", help="Path to the resulting MGF file.", type=Path)
    args = SimpleNamespace(**parser.parse_args().__dict__)


if __name__ == "__main__":
    duck_con = duckdb.connect()
    with open(args.config, "rb") as f:
        config = SimpleNamespace(**tomllib.load(f))

    if not config.silent:
        print("Using the following MGF config:")
        pprint(config)

    edges = read_df(args.edges)

    if not config.silent:
        print("Gathering precursor stats.")

    _precursor_stats_query = config.ms1.replace(
        "{precursor_stats_path}", str(args.precursor_stats)
    )
    precursor_stats = duckdb.query(_precursor_stats_query).df()

    if not config.silent:
        print("Gathering fragment stats.")

    _fragment_stats_query = config.ms2.replace(
        "{fragment_stats_path}", str(args.fragment_stats)
    )
    fragment_stats = duckdb.query(_fragment_stats_query).df()

    if not config.silent:
        print("Gathering spectra stats.")

    edges_idx = GroupedIndex(edges.MS1_ClusterID, edges)
    MS1_ClusterIDs = np.nonzero(edges_idx.counts)[0]

    def make_index(counts):
        index = np.empty(shape=(counts.shape[0] + 1,), dtype=np.uint64)
        index[0] = 0
        np.cumsum(counts, out=index[1:])
        return index

    _END_IONS_ = np.frombuffer(config.endions.encode("ascii"), np.uint8)

    @numba.njit(boundscheck=True, parallel=True)
    def get_spectra_byte_counts(
        MS1_ClusterIDs,
        MS1_ClusterID_to_start_fragments,
        MS1_ClusterID_to_fragments_cnt,
        MS1_ClusterID_to_header_len,
        MS2_ClusterIDs,
        MS2_ClusterID_to_mz_intensity_len,
        _END_IONS_len,
    ) -> npt.NDArray:
        byte_counts = np.zeros(shape=len(MS1_ClusterID_to_header_len), dtype=np.uint64)
        for i in numba.prange(len(MS1_ClusterIDs)):
            MS1_ClusterID = MS1_ClusterIDs[i]
            fragments_start = MS1_ClusterID_to_start_fragments[MS1_ClusterID]
            fragments_end = (
                fragments_start + MS1_ClusterID_to_fragments_cnt[MS1_ClusterID]
            )
            byte_cnt = MS1_ClusterID_to_header_len[MS1_ClusterID]
            for _MS2_ClusterID_idx_ in range(fragments_start, fragments_end):
                MS2_ClusterID = MS2_ClusterIDs[_MS2_ClusterID_idx_]
                byte_cnt += MS2_ClusterID_to_mz_intensity_len[MS2_ClusterID]
            byte_cnt += _END_IONS_len
            byte_counts[MS1_ClusterID] = byte_cnt
        return byte_counts

    if not config.silent:
        print("Calculating how many bytes each spectrum will take.")
    MS1_ClusterID_to_byte_cnt = get_spectra_byte_counts(
        MS1_ClusterIDs=MS1_ClusterIDs,
        MS1_ClusterID_to_start_fragments=edges_idx.index,
        MS1_ClusterID_to_fragments_cnt=edges_idx.counts,
        MS1_ClusterID_to_header_len=precursor_stats.header_len.to_numpy(),
        MS2_ClusterIDs=edges.MS2_ClusterID.to_numpy(),
        MS2_ClusterID_to_mz_intensity_len=fragment_stats.mz_intensity_len.to_numpy(),
        _END_IONS_len=len(_END_IONS_),
    )

    total_bytes = np.sum(MS1_ClusterID_to_byte_cnt[MS1_ClusterIDs])
    assert total_bytes == np.sum(MS1_ClusterID_to_byte_cnt)
    if not config.silent:
        print(f"The MGF will take {np.round(total_bytes / 1_000_000, 2):_} MB.")

    mgf = np.memmap(
        args.mgf_path,
        mode="w+",
        shape=total_bytes,
        dtype=np.uint8,
    )

    # TODO: add back m/z fragments sorting.
    @numba.njit(boundscheck=True, parallel=True)
    def write_spectra(
        mgf,
        MS1_ClusterIDs,
        MS1_ClusterID_to_start_fragments,
        MS1_ClusterID_to_fragments_cnt,
        MS1_ClusterID_to_header_starts,
        MS1_ClusterID_to_header_len,
        MS1_ClusterID_to_byte_idx,
        MS1_ClusterID_to_byte_cnt,
        MS1_headers,
        MS2_ClusterIDs,
        MS2_ClusterID_to_bytes,
        MS2_ClusterID_to_mz_intensity_starts,
        MS2_ClusterID_to_mz_intensity_len,
        _END_IONS_,
        progress_proxy,
    ) -> npt.NDArray:
        good = np.full(fill_value=False, shape=len(MS1_ClusterIDs), dtype=np.bool_)
        for i in numba.prange(len(MS1_ClusterIDs)):
            MS1_ClusterID = MS1_ClusterIDs[i]
            fragments_start = MS1_ClusterID_to_start_fragments[MS1_ClusterID]
            fragments_end = (
                fragments_start + MS1_ClusterID_to_fragments_cnt[MS1_ClusterID]
            )
            start_mgf_idx = mgf_idx = MS1_ClusterID_to_byte_idx[MS1_ClusterID]
            header_idx = MS1_ClusterID_to_header_starts[MS1_ClusterID]
            header_byte_cnt = MS1_ClusterID_to_header_len[MS1_ClusterID]

            for _ in range(header_byte_cnt):
                mgf[mgf_idx] = MS1_headers[header_idx]
                mgf_idx += np.uint64(1)
                header_idx += np.uint64(1)

            for _MS2_ClusterID_idx_ in range(fragments_start, fragments_end):
                MS2_ClusterID = MS2_ClusterIDs[_MS2_ClusterID_idx_]
                mz_intensity_idx = MS2_ClusterID_to_mz_intensity_starts[MS2_ClusterID]
                mz_intensity_byte_cnt = MS2_ClusterID_to_mz_intensity_len[MS2_ClusterID]

                for _ in range(mz_intensity_byte_cnt):
                    mgf[mgf_idx] = MS2_ClusterID_to_bytes[mz_intensity_idx]
                    mgf_idx += np.uint64(1)
                    mz_intensity_idx += np.uint64(1)

            for end_ion_byte in _END_IONS_:
                mgf[mgf_idx] = end_ion_byte
                mgf_idx += np.uint64(1)

            # check, if the total number of written bytes matches that that was anticipated.
            good[i] = np.bool_(
                (mgf_idx - start_mgf_idx) == MS1_ClusterID_to_byte_cnt[MS1_ClusterID]
            )

            progress_proxy.update(1)
        return good

    MS1_ClusterID_to_header_starts = make_index(precursor_stats.header_len.to_numpy())
    MS1_ClusterID_to_byte_idx = make_index(MS1_ClusterID_to_byte_cnt)
    MS1_ClusterID_to_bytes = np.frombuffer(
        precursor_stats.header.str.cat().encode("ascii"), dtype=np.uint8
    )
    MS2_ClusterID_to_bytes = np.frombuffer(
        fragment_stats.mz_intensity.str.cat().encode("ascii"), dtype=np.uint8
    )
    MS2_ClusterID_to_mz_intensity_starts = make_index(
        fragment_stats.mz_intensity_len.to_numpy()
    )

    if not config.silent:
        print("Dumping MGF to file in a hacky custom way.")

    with ProgressBar(total=len(MS1_ClusterIDs), desc="Dumping spectra") as progress:
        good = write_spectra(
            mgf=mgf,
            MS1_ClusterIDs=MS1_ClusterIDs,
            MS1_ClusterID_to_start_fragments=edges_idx.index,
            MS1_ClusterID_to_fragments_cnt=edges_idx.counts,
            MS1_ClusterID_to_header_starts=MS1_ClusterID_to_header_starts,
            MS1_ClusterID_to_header_len=precursor_stats.header_len.to_numpy(),
            MS1_ClusterID_to_byte_idx=MS1_ClusterID_to_byte_idx,
            MS1_ClusterID_to_byte_cnt=MS1_ClusterID_to_byte_cnt,
            MS1_headers=MS1_ClusterID_to_bytes,
            MS2_ClusterIDs=edges.MS2_ClusterID.to_numpy(),
            MS2_ClusterID_to_bytes=MS2_ClusterID_to_bytes,
            MS2_ClusterID_to_mz_intensity_starts=MS2_ClusterID_to_mz_intensity_starts,
            MS2_ClusterID_to_mz_intensity_len=fragment_stats.mz_intensity_len.to_numpy(),
            _END_IONS_=_END_IONS_,
            progress_proxy=progress,
        )
    assert np.all(good), "Some spectra took more bytes than anticipated."

    mgf.flush()

    if not config.silent:
        print("Dumped mgf.")

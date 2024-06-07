#!/usr/bin/env python3

import argparse
import re
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from midia_search_engines.sage_ops import (
    parse_MS1_ClusterIDs,
    parse_out_fragment_counts,
)

_development = False

if _development:
    from warnings import warn
    from IPython import get_ipython
    import pandas as pd
    from types import SimpleNamespace

    warn("Development mode")
    get_ipython().run_line_magic("load_ext", "autoreload")
    get_ipython().run_line_magic("autoreload", "2")
    pd.set_option("display.max_columns", None)

    folder = Path(
        "P/search/sage/G2cPAGSdaeOU87WWOevX6YQsCwxp0L_-__fONNG0jfrGv6-xFsicfyygxrL0n0SzTgixbpAAW4Z3MyzpGDhewBqdYeGUkQ8lVBsk-oGDU_Ty7WBI9MJdC-DLLSbPmnJBmxuUPMNRFf4EPaYM5WbRjEG5LnOBIweWJRFc5rqdKiWAQIe889LNmY7dMeR0Ljd7KYC8cPe50aqbeGmGpUVBCQ6eJTgtkuLgKArhPHZyUXUCo3qSVvGrWvAHMM9Xcfm/pEj8EF_Kj5AX7HyrePjlVhToAyCdDSvGLCwa-m7LJaJYESLiy0Dq_NQcQHn0AcIQGiQwMwnNKtypK43HpB2IANt7UjWR1bRUTIUAoUrm8LEImeQL0AgM2Q1VZfRBTK39z5qTA-fyjfD7osNSXghiuAFPbHKXkf4ThO5sx8yvJp678QilcznIg4OzJWqP3AEk9k2GbgA9O0e1RcWjKhTIc-s5JX7zob-PG7kZsRvx0t5fkC37Qr3fLWI_obI-/wQlnou96jBdNysZC9OXIFC7t_ThWpDB7O64Q79KOjnkDFc5nx6TnLINtl6VBAPhSaVjeDU59kNF_lM5kDJjStyyDbZelueAU="
    )

    other_folder = Path(
        "P/search/sage/G2MPAGSdWuN0Ia3Ot36dImRZwZAG_ev_zS3nVTzWu23_ELO2hNrEQkKsWVrpNB4xkKuHQogoDM4bcVE96wvTSkb2Yp2RByxUEwv4w1ZN0YvUftfohSXzweAWo4vEmEDHgiXPbS9zf1weLrJ0aIkxlOsiJbZGR1sKyJ59XnaZ4CDQT97n3GHUhdAYUljSoYl8oAt3L4UUVUdD0QwlC0Zg_2LBmV2crxrFIJzmThOqHjDKj00LfVVz_QDleTItP1S/gi2shP0te2P9QsvrWVBX4AGCfDDH4L0oc-Chls9GkCZDwwURp_5IKEB79BnCEbhIV6Gf9lK6VhOTjUDd0AFh7iAO3bD8rZkLAUKRzeVmMTPID2Avcjkgoiu6DDrWyh1F_chSXhfJ52AVbfSmMgQSU2mQwBPtjDN_Zgplfabt95RfK4bKRA4FkT1or-Q1IqhEVxQQ87C-iHuSrxJgwsp-/epemLG_JtnHC5EYoWPs3xEmyCD_y1ZmjzFpLtIRIsc3VVZ7RgWS4m1IdBkyCR-aeQiUrwsFkn7E8vJOppVDzERE_PCPqmmx4efa4oZdEYMSGj6UM2ovpBDIYT9E03sZsD"
    )

    args = SimpleNamespace()

    args.in_results_sage_tsv = folder / "results.sage.tsv"
    args.out_precursors = folder / "results.sage.parquet"
    args.out_fragments = folder / "matched_fragments.sage.parquet"
    recent_precursors = pd.read_parquet(other_folder / "results.sage.parquet")
    recent_fragments = pd.read_parquet(other_folder / "matched_fragments.sage.parquet")


else:
    parser = argparse.ArgumentParser(
        description="Calculate the ms2 stats using the fast_ms2_stats way."
    )
    parser.add_argument(
        "in_results_sage_tsv",
        help="Path to 'results.sage.tsv' with fragment info.",
        type=Path,
    )
    parser.add_argument(
        "out_precursors",
        help="Where to save `resuts.sage.tsv` translated into `parquet`.",
        type=Path,
    )
    parser.add_argument(
        "out_fragments",
        help="Where to save equivalent of `matched_fragments.sage.parquet`.",
        type=Path,
    )
    args = parser.parse_args()

if __name__ == "__main__":
    current_precursors = pd.read_csv(args.in_results_sage_tsv, sep="\t")

    current_precursors["psm_id"] = current_precursors[
        "MS1_ClusterID"
    ] = parse_MS1_ClusterIDs(current_precursors.scannr)

    recent_precursor_columns = [
        "psm_id",
        "filename",
        "scannr",
        "peptide",
        "stripped_peptide",
        "proteins",
        "num_proteins",
        "rank",
        "is_decoy",
        "expmass",
        "calcmass",
        "charge",
        "peptide_len",
        "missed_cleavages",
        "semi_enzymatic",
        "ms2_intensity",
        "isotope_error",
        "precursor_ppm",
        "fragment_ppm",
        "hyperscore",
        "delta_next",
        "delta_best",
        "rt",
        "aligned_rt",
        "predicted_rt",
        "delta_rt_model",
        "ion_mobility",
        "predicted_mobility",
        "delta_mobility",
        "matched_peaks",
        "longest_b",
        "longest_y",
        "longest_y_pct",
        "matched_intensity_pct",
        "scored_candidates",
        "poisson",
        "sage_discriminant_score",
        "posterior_error",
        "spectrum_q",
        "peptide_q",
        "protein_q",
        "reporter_ion_intensity",
    ]
    common_cols = list(set(recent_precursor_columns) & set(current_precursors.columns))

    translated = current_precursors[common_cols].copy()
    translated["is_decoy"] = current_precursors.proteins.str.contains("_rev")
    translated["stripped_peptide"] = translated.peptide.str.replace(
        r"\[.*?\]", "", regex=True
    )
    translated["semi_enzymatic"] = False
    translated["reporter_ion_intensity"] = None
    translated["delta_mobility"] = np.float32(0.0)
    translated["predicted_mobility"] = np.float32(0.0)
    translated["MS1_ClusterID"] = translated.psm_id
    translated = translated[list(recent_precursor_columns) + ["MS1_ClusterID"]]

    for col in translated.columns:
        if translated[col].dtype == "float64":
            translated[col] = translated[col].astype("float32")
        elif translated[col].dtype == "int64" and col != "psm_id":
            translated[col] = translated[col].astype("int32")

    translated.MS1_ClusterID = translated.MS1_ClusterID.astype("uint32")
    assert len(translated) > 0

    translated.sort_values("MS1_ClusterID", inplace=True, ignore_index=True)

    duckcon = duckdb.connect()

    fragments_to_parse = duckcon.query(
        """
    SELECT
    psm_id AS MS1_ClusterID,
    matched_y_exp_mz AS y_exp_mz,
    matched_y_calc_mz AS y_calc_mz,
    matched_b_exp_mz AS b_exp_mz,
    matched_b_calc_mz AS b_calc_mz,
    FROM 'current_precursors'
    """
    ).to_df()
    sage_fragment_counts, sage_fragments = parse_out_fragment_counts(fragments_to_parse)

    sage_fragments.rename(
        columns={
            "MS1_ClusterID": "psm_id",
            "fragment_tag": "fragment_type",
            "mz_calc": "fragment_mz_calculated",
            "mz_exp": "fragment_mz_experimental",
        },
        inplace=True,
    )
    sage_fragments.psm_id = sage_fragments.psm_id.astype("int64")

    for col in sage_fragments.columns:
        if sage_fragments[col].dtype == "float64":
            sage_fragments[col] = sage_fragments[col].astype("float32")
        elif sage_fragments[col].dtype == "int64" and col != "psm_id":
            sage_fragments[col] = sage_fragments[col].astype("int32")

    recent_fragment_columns = [
        "psm_id",
        "fragment_type",
        "fragment_ordinals",
        "fragment_charge",
        "fragment_mz_experimental",
        "fragment_mz_calculated",
        "fragment_intensity",
    ]
    sage_fragments = sage_fragments[
        [c for c in recent_fragment_columns if c in sage_fragments.columns]
    ]
    sage_fragments["MS1_ClusterID"] = sage_fragments.psm_id
    sage_fragments.MS1_ClusterID = sage_fragments.MS1_ClusterID.astype("uint32")
    sage_fragments.sort_values(
        ["MS1_ClusterID", "fragment_mz_experimental"], inplace=True, ignore_index=True
    )
    assert len(sage_fragments) > 0

    translated.to_parquet(args.out_precursors)
    sage_fragments.to_parquet(args.out_fragments)

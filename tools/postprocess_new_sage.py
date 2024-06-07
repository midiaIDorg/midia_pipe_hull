from collections import Counter
from pathlib import Path

import pandas as pd
from midia_search_engines.sage_ops import parse_MS1_ClusterIDs

pd.set_option("display.max_columns", None)

folder = Path(
    "P/search/sage/G48MQGSdWuNmB670-db2PyFkWcGQSkL5-se9c__0kd_8uzpPYIwNOAcH2GVZF0OIY3KDeDtRnpljHjgHX_BiSXSUSuVaMPMPGibpV_4M1igv_ItPWVuU3jyXSnNR4OpsM7D-WVwQRyDDeUUqmrJKWzPj_djoAziL-/0Bwdr5qHwPWfi6ovNsc5KR2J00aEavr12WeYNd8AwLq5AKTbSXLaJmgMThCredG0sfGKFVSL6BWeHRfzulLqqWo_tU2MaWRcBoPBvqHdSOEub2tRWIGmNjnSNV5GKbtDnWR2N713EI3d4yJBdg9iVjB-A42wa_BtiMuSBGjqHoDvlxRPHoHbJoCkho55-WHjCZEjU8v6urfbCfVCcn8OIU_6T-/NDK05c_BYOMgnwhK_2zAWWuO5VC4Y2cAhm_Ll1GhSe_qM_SMvlT74d4mnfbMni1JEHLjodrO12kr-TKd47iPHSlXiv2RAVPFnyyquBzF_RVyV9moH2dgFxjpCABK_G1BbOHx1LA=="
)
found_precursors = pd.read_parquet(folder / "results.sage.parquet")
found_precursors["MS1_ClusterID"] = parse_MS1_ClusterIDs(found_precursors.scannr)
found_precursors.sort_values("MS1_ClusterID", inplace=True, ignore_index=True)


psm_id_2_MS1_ClusterID = dict(
    zip(found_precursors.psm_id, found_precursors.MS1_ClusterID)
)
found_fragments = pd.read_parquet(folder / "matched_fragments.sage.parquet")
found_fragments["MS1_ClusterID"] = found_fragments.psm_id.map(psm_id_2_MS1_ClusterID)

found_fragments.sort_values(
    ["MS1_ClusterID", "fragment_mz_experimental"], ignore_index=True, inplace=True
)
found_fragments.to_parquet()

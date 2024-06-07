import sys
from math import floor

import pandas as pd
from pyteomics.mass.unimod import Unimod

_, out_location, *_ = sys.argv

unimod = pd.DataFrame(
    dict(
        id=mod.id,
        monoisotopic_mass=mod.monoisotopic_mass,
        ex_code_name=mod.ex_code_name,
        full_name=mod.full_name,
        composition=str(mod.composition),
        code_name=mod.code_name,
        username_of_poster=mod.username_of_poster,
        average_mass=mod.average_mass,
    )
    for mod in Unimod()
)
# unimod = pd.read_csv("data/unimod.csv")

unimod["tag"] = [f"[UNIMOD:{_id}]" for _id in unimod.id]
unimod["short_tag"] = [f"[U:{_id}]" for _id in unimod.id]
unimod["machine_monoisotopic_mass"] = [
    floor(monoisotopic_mass * 2**13) * 2 ** (-13)
    for monoisotopic_mass in unimod.monoisotopic_mass
]
unimod.to_csv(out_location, index=False)

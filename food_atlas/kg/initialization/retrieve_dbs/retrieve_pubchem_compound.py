from ast import literal_eval

import pandas as pd
from Bio import Entrez

with open("food_atlas/kg/api_key.txt") as f:
    Entrez.email = f.readline().strip()
    Entrez.api_key = f.readline().strip()


if __name__ == '__main__':
    data = pd.read_csv(
        "outputs/kg/initialization/_pubchem_cids_fdc_manual.tsv", sep='\t',
    )['pubchem_cid'].dropna()

    pubchem_cids = []
    for cids in data:
        if ',' in cids:
            pubchem_cids += literal_eval(cids)
        else:
            pubchem_cids.append(int(cids))

    pubchem_cids = list(set(pubchem_cids))

    cids_str = ','.join([str(x) for x in pubchem_cids])
    handler = Entrez.esummary(db='pccompound', id=cids_str)
    records = Entrez.read(handler)
    pd.DataFrame(records).to_csv(
        "outputs/kg/initialization/_pubchem_compound.tsv",
        sep='\t',
        index=False,
    )

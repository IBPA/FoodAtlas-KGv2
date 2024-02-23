from ast import literal_eval

import pandas as pd
from Bio import Entrez
# import pubchempy as pcp
from tqdm import tqdm

with open("food_atlas/kg/api_key.txt") as f:
    Entrez.email = f.readline().strip()
    Entrez.api_key = f.readline().strip()

# def get_pubchem_compound(cid):
#     c = pcp.Compound.from_cid(cid)
#     return {
#         'pubchem_cid': c.cid,
#         'iupac_name': c.iupac_name,
#         'synonyms': c.synonyms,
#     }


if __name__ == '__main__':
    data = pd.read_csv(
        "outputs/kg/initialization/pubchem_cids_fdc_manual.tsv", sep='\t',
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
        "outputs/kg/initialization/pubchem_compound.tsv",
        sep='\t',
        index=False,
    )

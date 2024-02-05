from ast import literal_eval

import pandas as pd
import pubchempy as pcp
from tqdm import tqdm


def get_pubchem_compound(cid):
    c = pcp.Compound.from_cid(cid)
    return {
        'pubchem_cid': c.cid,
        'iupac_name': c.iupac_name,
        'synonyms': c.synonyms,
    }


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
    records = []
    for cid in tqdm(pubchem_cids):
        records += [get_pubchem_compound(cid)]
    data = pd.DataFrame(records)

    data.to_csv("outputs/kg/initialization/pubchem_compound.tsv", sep='\t')

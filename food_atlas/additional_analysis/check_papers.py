from ast import literal_eval

import pandas as pd


if __name__ == '__main__':
    fm_garlic = pd.read_pickle(
        "data/FoodMine/garlic_food_data.pkl"
    )

    pmcids_fm_garlic = pd.read_csv(
        "data/FoodMine/ids_garlic.csv",
    ).query("PMID.notna() & PMID != 0")['PMCID'].tolist()

    fa_garlic = pd.read_csv(
        "outputs/kg/metadata_contains.tsv",
        sep='\t',
        converters={
            'reference': literal_eval
        }
    )
    fa_garlic['pmcid'] = fa_garlic['reference'].apply(
        lambda reference: reference['pmcid'] if 'pmcid' in reference else None
    )
    pmcids_fa_garlic = fa_garlic['pmcid'].dropna().unique().tolist()

    for pmcid in pmcids_fm_garlic:
        if int(pmcid[3:]) not in pmcids_fa_garlic:
            print(pmcid)

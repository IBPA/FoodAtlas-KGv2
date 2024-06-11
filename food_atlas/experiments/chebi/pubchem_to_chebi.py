import numpy as np
import pandas as pd


if __name__ == '__main__':
    metadata = pd.read_csv("outputs/kg/metadata_contains.tsv", sep='\t')
    chemical_names = metadata['_chemical_name'].unique().tolist()
    chemical_names = [x for x in chemical_names if not x.startswith('_FDC_Nutrient')]

    # # Download names.
    # with open("chemical_names.tsv", 'w') as f:
    #     for name in chemical_names:
    #         f.write(f"{name}\n")

    # CIDS.
    # chemical_names = pd.read_csv(
    #     "chemical_names_cids.txt", sep='\t', header=None, names=['name', 'cid']
    # ).astype({'cid': 'Int64'})
    # cids = chemical_names['cid'].dropna().unique().tolist()
    # with open("cids.txt", 'w') as f:
    #     for cid in cids:
    #         f.write(f"{cid}\n")

    # Approach 1.
    chemical_names_1 = pd.read_csv(
        "name_chebi.txt", sep='\t', header=None, names=['name', 'chebi']
    )
    chemical_names_1 \
        = chemical_names_1.groupby('name')['chebi'].apply(list).reset_index()
    print(chemical_names_1)

    # Approach 2.
    chemical_names_2 = pd.read_csv(
        "chemical_names_cids.txt", sep='\t', header=None, names=['name', 'cid']
    ).astype({'cid': 'Int64'})

    map_cid_to_chebi = pd.read_csv(
        "cid_chebi.txt", sep='\t', header=None, names=['cid', 'chebi']
    ).astype({'cid': 'Int64'})
    map_cid_to_chebi \
        = map_cid_to_chebi.groupby('cid')['chebi'].apply(list).reset_index()

    chemical_names_2 = chemical_names_2.merge(
        map_cid_to_chebi, left_on='cid', right_on='cid', how='left'
    )

    chemical_names = chemical_names_1.merge(
        chemical_names_2, left_on='name', right_on='name', how='left'
    )
    chemical_names['chebi_x'] = chemical_names['chebi_x'].apply(
        lambda x: np.nan if type(x) == list and pd.isna(x[0]) else x
    )
    chemical_names['chebi_y'] = chemical_names['chebi_y'].apply(
        lambda x: np.nan if type(x) == list and pd.isna(x[0]) else x
    )
    print(chemical_names)
    chemical_names.to_csv("chemical_names_chebi.tsv", sep='\t', index=False)
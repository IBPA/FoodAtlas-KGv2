import numpy as np
import pandas as pd


if __name__ == '__main__':
    # Load mentions.
    metadata = pd.read_csv("outputs/kg/metadata_contains.tsv", sep='\t')
    mentions = metadata['_chemical_name'].unique().tolist()
    mentions = [x for x in mentions if not x.startswith('_FDC_Nutrient')]

    # mentions = pd.DataFrame(mentions, columns=['NAME'])
    # mentions['ChEBI_direct'] = None
    # mentions['ChEBI_PubChem'] = None
    # mentions = mentions.set_index('NAME')

    # # Direct.
    # compounds = pd.read_csv("data/ChEBI/compounds.tsv", sep='\t', encoding='latin1')
    # names_chebi = pd.read_csv("data/ChEBI/names.tsv", sep='\t')
    # names_compounds = compounds[['ID', 'NAME']].dropna(subset=['NAME'])
    # names_compounds.columns = ['COMPOUND_ID', 'NAME']
    # names_chebi = pd.concat([names_chebi, names_compounds], axis=0)
    # names_chebi['NAME'] = names_chebi['NAME'].str.lower()
    # names_chebi = names_chebi.groupby('NAME')['COMPOUND_ID'].apply(
    #     lambda x: list(set(x))
    # ).reset_index()
    # names_chebi_i = names_chebi[names_chebi['NAME'].isin(mentions.index)]
    # mentions.loc[names_chebi_i['NAME'], ['ChEBI_direct']] \
    #     = names_chebi_i['COMPOUND_ID'].values

    # # PubChem.
    # names_chebi = pd.read_csv(
    #     "name_chebi.txt",
    #     sep='\t',
    #     header=None,
    #     names=['name', 'chebi'],
    # )
    # names_chebi = names_chebi[names_chebi['name'].isin(mentions.index)]

    # def get_list(x):
    #     x = list(x)
    #     if len(x) == 1 and pd.isna(x[0]):
    #         return np.nan
    #     return [int(xx.split(':')[-1]) for xx in x]

    # names_chebi = names_chebi.groupby('name')['chebi'].apply(get_list).reset_index()
    # mentions.loc[names_chebi['name'], ['ChEBI_PubChem']] = names_chebi['chebi'].values
    # print(mentions)

    # # Statistics.
    # print(mentions['ChEBI_direct'].notna().sum())
    # print(mentions['ChEBI_PubChem'].notna().sum())

    # mentions.to_csv("mentions_chebi.tsv", sep='\t')

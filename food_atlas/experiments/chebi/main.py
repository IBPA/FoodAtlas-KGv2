from itertools import combinations

import numpy as np
import pandas as pd
from tqdm import tqdm

tqdm.pandas()

def is_reachable(s, e, map_is_a):
    if s == e:
        return True
    if s not in map_is_a:
        return False
    for x in map_is_a[s]:
        if is_reachable(x, e, map_is_a):
            return True

def load_map_is_a():
    # entities = pd.read_csv("data/ChEBI/compounds.tsv", sep='\t', encoding='latin1')
    triplets = pd.read_csv("data/ChEBI/relation.tsv", sep='\t').query(
        "TYPE in ['is_a'] & STATUS == 'C'"
        # "TYPE in ['is_a', 'has_part'] & STATUS == 'C'"
    )

    # Note: ChEBI has flipped head and tail for is_a relationship.
    map_is_a = {}
    for _, row in triplets.iterrows():
        if row['TYPE'] == 'is_a':
            head, tail = row['FINAL_ID'], row['INIT_ID']
        elif row['TYPE'] == 'has_part':
            head, tail = row['INIT_ID'], row['FINAL_ID']

        if head not in map_is_a:
            map_is_a[head] = []
        map_is_a[head].append(tail)

    return map_is_a


if __name__ == '__main__':
    # Load mentions.
    metadata = pd.read_csv("outputs/kg/metadata_contains.tsv", sep='\t')
    mentions = metadata['_chemical_name'].tolist()
    mentions = [x for x in mentions if not x.startswith('_FDC_Nutrient')]

    cnt = {}
    for mention in mentions:
        cnt[mention] = cnt.get(mention, 0) + 1

    mentions = metadata['_chemical_name'].unique().tolist()
    mentions = [x for x in mentions if not x.startswith('_FDC_Nutrient')]
    mentions = pd.DataFrame(mentions, columns=['NAME'])
    mentions['count'] = mentions['NAME'].apply(lambda x: cnt[x])
    mentions = mentions.set_index('NAME')
    mentions = mentions.sort_values('count', ascending=False)

    # Level 1. Exact match to a 3-star compound.
    compounds = pd.read_csv("data/ChEBI/compounds.tsv", sep='\t', encoding='latin1')
    compounds_3star = compounds[compounds['STAR'] == 3]
    compounds_3star = compounds_3star[['ID', 'NAME']].dropna(subset=['NAME'])
    compounds_3star['NAME'] = compounds_3star['NAME'].str.lower()
    assert compounds_3star['NAME'].duplicated().sum() == 0

    compounds_3star = compounds_3star.set_index('NAME')
    mentions['ChEBI_3star_exact'] = mentions.index.map(
        lambda x: [compounds_3star.loc[x, 'ID']] if x in compounds_3star.index else []
    )

    # Level 2. Synonym match to a 3-star compound.
    map_is_a = load_map_is_a()
    synonyms = pd.read_csv("data/ChEBI/names.tsv", sep='\t')
    synonyms_3star = synonyms[synonyms['COMPOUND_ID'].isin(
        compounds_3star['ID']
    )].copy()
    synonyms_3star['NAME'] = synonyms_3star['NAME'].str.lower()
    synonyms_3star = synonyms_3star.groupby('NAME')['COMPOUND_ID'].apply(
        lambda x: list(set(x))
    ).reset_index()
    synonyms_3star = synonyms_3star.set_index('NAME')

    def assign_chebi_id(x, disambiguate=False):
        if x not in synonyms_3star.index:
            return []

        ids = synonyms_3star.loc[x, 'COMPOUND_ID']
        if len(ids) == 1:
            return ids
        else:
            if disambiguate:
                if len(ids) == 2:
                    s, e = ids
                    s_reaches_e = is_reachable(s, e, map_is_a)
                    e_reaches_s = is_reachable(e, s, map_is_a)
                    if s_reaches_e and e_reaches_s:
                        raise ValueError(f"Loop exists: {x}")
                    if s_reaches_e:
                        return [e]
                    if e_reaches_s:
                        return [s]
            return ids

    mentions['ChEBI_3star_synonym'] = mentions.index.map(assign_chebi_id)
    mentions['ChEBI_3star_synonym_dis'] \
        = mentions.index.map(lambda x: assign_chebi_id(x, True))

    # # Pick the best IDs for elementals.
    # compounds_3star_ = compounds_3star.reset_index().set_index('ID')

    # def assign_elemental_id(row):
    #     names = compounds_3star_.loc[row['ChEBI_3star_synonym_dis'], 'NAME'].tolist()

    #     # Check if any of the names is an elemental.
    #     for name in names:
    #         if name.endswith(' atom'):
    #             print(row.name)
    #             print(names)
    #             print()

    # mentions['ChEBI_3star_synonym_dis'] = mentions.apply(assign_elemental_id, axis=1)


    # Level 3. From PubChem to 3-star.
    names_chebi = pd.read_csv(
        "name_chebi.txt",
        sep='\t',
        header=None,
        names=['name', 'chebi'],
    )
    names_chebi = names_chebi[names_chebi['name'].isin(mentions.index)]
    names_chebi = names_chebi.groupby('name')['chebi'].apply(list)

    names_cid = pd.read_csv(
        "name_cid.txt",
        sep='\t',
        header=None,
        names=['name', 'cid'],
    ).astype({'cid': 'Int64'})
    names_cid = names_cid[names_cid['name'].isin(mentions.index)]
    names_cid = names_cid.groupby('name')['cid'].apply(list)

    def assign_pubchem_cid(x):
        if x not in names_cid.index:
            return []

        ids = names_cid.loc[x]
        if len(ids) == 1 and pd.isna(ids[0]):
            return []

        return ids

    mentions['PubChem_CID'] = mentions.index.map(assign_pubchem_cid)

    def assign_chebi_id(x):
        if x not in names_chebi.index:
            return []

        ids = names_chebi.loc[x]
        if len(ids) == 1 and pd.isna(ids[0]):
            return []

        ids = [int(xx.split(':')[-1]) for xx in ids]
        ids = [xx for xx in ids if xx in compounds_3star['ID'].values]

        return ids

    mentions['ChEBI_3star_PubChem'] = mentions.index.map(assign_chebi_id)

    def assign_chebi_final(row):
        if len(row['ChEBI_3star_exact']) == 1:
            return row['ChEBI_3star_exact'][0]

        if len(row['ChEBI_3star_synonym_dis']) == 1:
            return row['ChEBI_3star_synonym_dis'][0]

        if len(row['ChEBI_3star_PubChem']) == 1:
            return row['ChEBI_3star_PubChem'][0]

    mentions['ChEBI_final'] = mentions.apply(assign_chebi_final, axis=1)
    mentions.to_csv("mentions_chebi.tsv", sep='\t')

    exit()
    names_compounds = compounds[['ID', 'NAME']].dropna(subset=['NAME'])
    names_compounds.columns = ['COMPOUND_ID', 'NAME']
    names_chebi = pd.concat([names_chebi, names_compounds], axis=0)
    names_chebi['NAME'] = names_chebi['NAME'].str.lower()
    names_chebi = names_chebi.groupby('NAME')['COMPOUND_ID'].apply(
        lambda x: list(set(x))
    ).reset_index()
    names_chebi_i = names_chebi[names_chebi['NAME'].isin(mentions.index)]
    mentions.loc[names_chebi_i['NAME'], ['ChEBI_direct']] \
        = names_chebi_i['COMPOUND_ID'].values

    # PubChem.

    print(mentions)

    # Statistics.
    print(mentions['ChEBI_direct'].notna().sum())
    print(mentions['ChEBI_PubChem'].notna().sum())

    mentions.to_csv("mentions_chebi.tsv", sep='\t')

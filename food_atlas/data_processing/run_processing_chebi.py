# -*- coding: utf-8 -*-
"""

Load ChEBI for chemical entities.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""
from collections import OrderedDict

import pandas as pd


def _load_map_is_a() -> dict:
    """Load ChEBI is_a relationship.

    Returns:
        dict: A mapping from ChEBI ID to its parents.

    """
    triplets = pd.read_csv("data/ChEBI/relation.tsv", sep='\t').query(
        "TYPE in ['is_a']"
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


def _label_is_chemical_entity(chebi: pd.DataFrame) -> pd.DataFrame:
    """Label ChEBI entry as chemical if has an ancestor of `chemical entity`.

    Args:
        chebi (pd.DataFrame): The ChEBI entries.

    Returns:
        pd.DataFrame: The ChEBI entries with the `is_chemical` label.

    """
    map_is_a = _load_map_is_a()

    def _is_chemical_entity(row):
        """Deapth-first search to label ChEBI entry as chemical."""

        nonlocal visited

        def dfs(chebi_id):
            if chebi_id in visited:
                return visited[chebi_id]
            if chebi_id not in map_is_a:
                return False

            results = []
            for parent in map_is_a[chebi_id]:
                results += [dfs(parent)]

            if any(results):
                visited[chebi_id] = True
                return True
            else:
                visited[chebi_id] = False
                return False

        dfs(row.name)

    # Label molecular entity.
    MOLECULAR_ENTITY = 23367  # Molecular entity.
    visited = {}
    visited[MOLECULAR_ENTITY] = True
    chebi.apply(_is_chemical_entity, axis=1)
    chebi['is_molecular_entity'] = chebi.index.map(visited)

    # # Label atom.
    # ATOM = 33250  # atom.
    # visited = {}
    # visited[ATOM] = True
    # chebi.apply(_is_chemical_entity, axis=1)
    # chebi['is_atom'] = chebi.index.map(visited)

    return chebi


def _load_chebi() -> pd.DataFrame:
    """Load and clean ChEBI data by removing unused compounds.

    Returns:
        pd.DataFrame: The cleaned ChEBI data.

    """
    chebi = pd.read_csv(
        "data/ChEBI/compounds.tsv", sep='\t', encoding='latin1'
    ).set_index('ID')
    chebi['NAME'] = chebi['NAME'].str.lower().str.strip()

    # Drop outdated compounds.
    chebi = chebi.query("PARENT_ID.isna()").copy()

    # Only keep compounds under 'chemical entity'.
    chebi = _label_is_chemical_entity(chebi)
    chebi['is_molecular_entity'] = chebi['is_molecular_entity'].fillna(False)
    chebi = chebi.query("is_molecular_entity").copy()

    return chebi


if __name__ == '__main__':
    """"""
    # Load ChEBI data.
    chebi = _load_chebi()

    # Manual fix some issues.
    chebi.at[221398, 'NAME'] = '15G256nu'
    chebi.at[224404, 'NAME'] = '15G256omicron'
    chebi = chebi.drop(index=194466)
    assert chebi['NAME'].duplicated().sum() == 0

    chebi_synonyms = pd.read_csv("data/ChEBI/names.tsv", sep='\t')
    chebi_synonyms = chebi_synonyms.query("LANGUAGE == 'en'").copy()
    chebi_synonyms['NAME'] = chebi_synonyms['NAME'].str.lower().str.strip()
    chebi_synonyms = chebi_synonyms[chebi_synonyms['COMPOUND_ID'].isin(chebi.index)]

    # cdno = pd.read_csv("outputs/data_processing/cdno_cleaned.tsv", sep='\t')
    # chebi = chebi.set_index('CHEBI_ACCESSION')
    # cdno['chebi_id'] = cdno['chebi_id'].apply(
    #     lambda x: f"CHEBI:{x.split('CHEBI_')[-1]}" if pd.notna(x) else None
    # )
    # cdno = cdno.dropna(subset=['chebi_id'])
    # chebi_cdno = chebi.loc[cdno['chebi_id']]
    # chebi_cdno.to_csv("chebi_cdno.tsv", sep='\t')

    lut_chemical ={}
    # for _, row in chebi.iterrows():
    #     if row['NAME'] not in lut_chemical:
    #         lut_chemical[row['NAME']] = []
    #     lut_chemical[row['NAME']] += [row.name]
    # for _, row in chebi_synonyms.iterrows():
    #     if row['NAME'] not in lut_chemical:
    #         lut_chemical[row['NAME']] = []
    #     lut_chemical[row['NAME']] += [row['COMPOUND_ID']]

    # Level 1. 3-star molecular entries.
    chebi_3star = chebi[chebi['STAR'] == 3]
    # - ChEBI Names.
    for _, row in chebi_3star.iterrows():
        if row['NAME'] not in lut_chemical:
            lut_chemical[row['NAME']] = [row.name]
    # - Synonyms.
    chebi_synonyms_3star = chebi_synonyms[chebi_synonyms['COMPOUND_ID'].isin(
        chebi_3star.index
    )]
    for _, row in chebi_synonyms_3star.iterrows():
        if row['NAME'] not in lut_chemical:
            lut_chemical[row['NAME']] = OrderedDict.fromkeys([row['COMPOUND_ID']])
        else:
            if isinstance(lut_chemical[row['NAME']], list):
                pass
            else:
                lut_chemical[row['NAME']][row['COMPOUND_ID']] = None
    lut_chemical = {
        k: list(OrderedDict.fromkeys(v).keys())
        for k, v in lut_chemical.items()
    }

    # Level 2. 2-star molecular entries.
    chebi_2star = chebi[chebi['STAR'] == 2]
    # - ChEBI Names.
    for _, row in chebi_2star.iterrows():
        if row['NAME'] not in lut_chemical:
            lut_chemical[row['NAME']] = [row.name]
    # - Synonyms.
    chebi_synonyms_2star = chebi_synonyms[chebi_synonyms['COMPOUND_ID'].isin(
        chebi_2star.index
    )]
    for _, row in chebi_synonyms_2star.iterrows():
        if row['NAME'] not in lut_chemical:
            lut_chemical[row['NAME']] = OrderedDict.fromkeys([row['COMPOUND_ID']])
        else:
            if isinstance(lut_chemical[row['NAME']], list):
                pass
            else:
                lut_chemical[row['NAME']][row['COMPOUND_ID']] = None
    lut_chemical = {
        k: list(OrderedDict.fromkeys(v).keys())
        for k, v in lut_chemical.items()
    }
    lut_chemical = pd.DataFrame(
        lut_chemical.items(),
        columns=['NAME', 'CHEBI_ID'],
    )

    lut_chemical.to_csv(
        "outputs/data_processing/chebi_name_to_id.tsv",
        sep='\t',
        index=False,
    )
    chebi.to_csv(
        "outputs/data_processing/chebi_cleaned.tsv",
        sep='\t',
    )

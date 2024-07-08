# -*- coding: utf-8 -*-
"""

Load ChEBI for chemical entities.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""
from ast import literal_eval

import pandas as pd


def load_mapper_name_to_chebi_id() -> pd.DataFrame:
    """Load mapper from mention to ChEBI ID.

    Returns:
        pd.DataFrame: Mapper from mention to ChEBI ID.

    """
    name2chebi_id = pd.read_csv(
        "outputs/data_processing/chebi_name_to_id.tsv",
        sep='\t',
        converters={'CHEBI_ID': literal_eval},
    )

    # Manual correction: Remove 'ash' due to ambiguity.
    names_remove = ['ash']
    name2chebi_id = name2chebi_id[~name2chebi_id['NAME'].isin(names_remove)].copy()

    return name2chebi_id


def load_mapper_chebi_id_to_names() -> pd.DataFrame:
    """Load mapper from ChEBI ID to mention.

    Returns:
        pd.DataFrame: Mapper from ChEBI ID to mention.

    """
    mapper = {}
    name2chebi_id = load_mapper_name_to_chebi_id().set_index('NAME')

    def add_to_mapper(row):
        nonlocal mapper

        for chebi_id in row['CHEBI_ID']:
            if chebi_id not in mapper:
                mapper[chebi_id] = []
            mapper[chebi_id] += [row.name]

    name2chebi_id.progress_apply(add_to_mapper, axis=1)
    name2chebi_id = pd.Series(mapper).reset_index().rename(
        columns={0: 'NAME', 'index': 'CHEBI_ID'}
    )

    return name2chebi_id

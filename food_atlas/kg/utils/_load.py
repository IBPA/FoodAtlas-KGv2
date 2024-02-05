from ast import literal_eval

import pandas as pd


def load_lookup_tables():
    luts = []
    for suffix in ['food', 'chemical']:
        lut_df = pd.read_csv(
            f"outputs/kg/lookup_table_{suffix}.tsv",
            sep='\t',
            converters={'foodatlas_id': literal_eval},
        )
        lut = dict(zip(lut_df['name'], lut_df['foodatlas_id']))
        luts += [lut]

    return luts


def load_entities(
    path_file: str = "outputs/kg/entities.tsv"
):
    return pd.read_csv(
        path_file,
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    )

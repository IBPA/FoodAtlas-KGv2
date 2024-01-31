from ast import literal_eval

import pandas as pd


def load_food_lookup_table():
    lut_df = pd.read_csv(
        "outputs/kg/food_lookup_table.tsv",
        sep='\t',
        converters={'foodatlas_id': literal_eval},
    )
    lut = dict(zip(lut_df['name'], lut_df['foodatlas_id']))

    return lut


def load_food_entities():
    return pd.read_csv(
        "outputs/kg/food_entities.tsv",
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    )

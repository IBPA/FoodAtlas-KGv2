# -*- coding: utf-8 -*-
"""

Initialize food entities based on FoodOn.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""
import pandas as pd
from pandarallel import pandarallel

from ._load_foodon import load_foodon, load_lut_food
from ...utils import constants

pandarallel.initialize(progress_bar=True)


if __name__ == '__main__':
    foodon = load_foodon()
    foodon_food = foodon[foodon['is_food']]
    lut_food = load_lut_food()

    entities = foodon_food.reset_index()
    columns = [
        'foodatlas_id',
        'entity_type',
        'common_name',
        'scientific_name',
        'synonyms',
        'external_ids',
    ]
    entities[columns] = None

    lut_food_df = pd.DataFrame(lut_food.items(), columns=['name', 'foodon_id'])
    names_grouped = lut_food_df.groupby('foodon_id')['name'].apply(list)

    entities['foodatlas_id'] = [f"e{i}" for i in list(range(1, len(entities) + 1))]
    entities['entity_type'] = 'food'
    entities['synonyms'] = entities['foodon_id'].map(names_grouped)
    entities['synonyms'] = entities.apply(
        lambda row: row['synonyms']
        + [constants.get_lookup_key_by_id('foodon_id', row['foodon_id'])],
        axis=1,
    )
    entities['common_name'] = entities['synonyms'].apply(lambda x: x[0])
    entities['external_ids'] = entities['foodon_id'].apply(lambda x: {'foodon_id': x})

    map_foodon_to_faid = {}
    for i, row in entities.iterrows():
        map_foodon_to_faid[row['external_ids']['foodon_id']] = row['foodatlas_id']

    lut_food_df['foodatlas_id'] = lut_food_df['foodon_id'].apply(
        lambda foodon_id: [str(
            map_foodon_to_faid[foodon_id]
        )]
    )

    # Append the foodon_id to the lookup table.
    lut_food_new_rows = []
    for k, v in map_foodon_to_faid.items():
        lut_food_new_rows += [{
            'name': constants.get_lookup_key_by_id('foodon_id', k),
            'foodatlas_id': [v],
        }]
    lut_food_df = pd.concat([
        lut_food_df,
        pd.DataFrame(lut_food_new_rows)
    ])

    entities = entities[columns].set_index('foodatlas_id')
    entities.to_csv("outputs/kg/entities.tsv", sep='\t')

    lut_food_df = lut_food_df[['name', 'foodatlas_id']]
    lut_food_df.to_csv("outputs/kg/lookup_table_food.tsv", sep='\t', index=False)

import os
from ast import literal_eval
from collections import defaultdict

import pandas as pd


if __name__ == '__main__':
    if os.path.exists("outputs/kg/food_lookup_table.pkl"):
        raise Exception("Food lookup table already exists.")

    lut = defaultdict(list)
    entities = pd.read_csv(
        "outputs/kg/food_entities.tsv",
        sep='\t',
        converters={'synonyms': literal_eval}
    )

    def _add_to_lut(row, lut):
        for name in row['synonyms']:
            name = name.lower()
            lut[name] += [row['foodatlas_id']]

    entities.apply(lambda row: _add_to_lut(row, lut), axis=1)

    pd.DataFrame(lut.items(), columns=['name', 'foodatlas_id']).to_csv(
        "outputs/kg/food_lookup_table.tsv", sep='\t', index=False
    )

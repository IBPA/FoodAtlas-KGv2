import csv

import numpy as np
import pandas as pd

from ..kg import KnowledgeGraph

# # Check all sysnonyms given one entity can be linked back to the entity in LUT.
# pass

# # Check all LUT entities are in the entities.
# pass

# # Check triplets from Lit2KG

def load_pilot_triplets():
    triplets = pd.read_excel(
        "food_atlas/tests/evaluation_v1_gpt3.5_gpt4.xlsx",
        sheet_name="Sheet1 (2)",
    )
    triplets = triplets.dropna(subset=['GPT4'])
    triplets['reference'] = triplets['sentence'].ffill()

    def _tripletify(s):
        nonlocal errors

        if pd.isna(s):
            return ['', '', '', '']

        s = next(csv.reader([s], delimiter=',', quotechar='"', skipinitialspace=True))
        if len(s) != 4:
            errors += [s]
            return ['', '', '', '']

        return s

    errors = []
    triplets['triplets'] = triplets['GPT4'].apply(_tripletify)
    triplets[['head', 'food_part', 'tail', 'conc']] = pd.DataFrame(
        triplets['triplets'].tolist(), index=triplets.index
    ).apply(lambda col: col.str.strip().str.lower())
    triplets['_extracted_conc'] = triplets['conc']
    triplets['_extracted_food_part'] = triplets['food_part']

    triplets['conc_value'] = None
    triplets['conc_unit'] = None
    triplets['food_part'] = None
    triplets['food_processing'] = None
    triplets['source'] = 'PILOT'
    triplets['quality_score'] = None
    triplets['relationship'] = 'contains'
    triplets = triplets[[
        'head', 'relationship', 'tail', 'conc_value', 'conc_unit', 'food_part',
        'food_processing', 'source', 'reference', 'quality_score', '_extracted_conc',
        '_extracted_food_part',
    ]]

    return triplets


if __name__ == '__main__':
    triplets = load_pilot_triplets()
    kg = KnowledgeGraph()
    kg.add_triplets(triplets)


    # lut_food, lut_chem = load_lookup_tables()

    # foods = triplets.loc[triplets['food'] != '', 'food'].unique()
    # n_not_in_lut = 0
    # for food in foods:
    #     if food not in lut_food:
    #         # print(food)
    #         n_not_in_lut += 1
    # print(f"{n_not_in_lut}/{len(foods)} unique foods not in LUT")

    # chems = triplets.loc[triplets['chem'] != '', 'chem'].unique()
    # n_not_in_lut = 0
    # for chem in chems:
    #     if chem not in lut_chem:
    #         n_not_in_lut += 1
    #     else:
    #         print(chem)

    # print(f"{n_not_in_lut}/{len(chems)} unique chems not in LUT")

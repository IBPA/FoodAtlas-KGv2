from itertools import product

import pandas as pd

from ..utils import load_lookup_tables, constants


def get_triplets():
    food = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food.csv",
        usecols=['fdc_id', 'description'],
    ).set_index('fdc_id')
    nutrient = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/nutrient.csv",
        usecols=['id', 'name', 'unit_name'],
    ).set_index('id')
    food_nutrient = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food_nutrient.csv",
        usecols=['id', 'fdc_id', 'nutrient_id', 'amount'],
    )
    fdc_ids_ff = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/foundation_food.csv"
    )['fdc_id']
    food_nutrient = food_nutrient[food_nutrient['fdc_id'].isin(fdc_ids_ff)]
    food_nutrient = food_nutrient.query("nutrient_id != 2066").copy()
    food_nutrient['food_name'] = food.loc[food_nutrient['fdc_id'], 'description'].values
    food_nutrient['food_name'] = food_nutrient['food_name'].str.strip().str.lower()
    food_nutrient['chem_name'] \
        = nutrient.loc[food_nutrient['nutrient_id'], 'name'].values
    food_nutrient['chem_name'] = food_nutrient['chem_name'].str.strip().str.lower()

    lut_food, lut_chem = load_lookup_tables()

    def _get_triplet(row):
        nonlocal triplets, containing, foodatlas_tid_curr, foodatlas_mid_curr

        fdc_id = row['fdc_id']
        fdc_nutrient_id = row['nutrient_id']
        key_food = constants.LOOKUP_BY_ID.format("fdc_ids", fdc_id)
        key_chem = constants.LOOKUP_BY_ID.format("fdc_nutrient_ids", fdc_nutrient_id)
        if key_chem not in lut_chem:
            print(f"Chemical not found: {key_chem}")
            return

        conc_value = row['amount']
        conc_unit = f"{nutrient.loc[fdc_nutrient_id, 'unit_name'].lower()}/100g"
        containing_ = {
            'foodatlas_id': f"mc{foodatlas_mid_curr}",
            'tids': [],
            'food_name': food.loc[fdc_id, 'description'].strip().lower(),
            'chemical_name': nutrient.loc[fdc_nutrient_id, 'name'].strip().lower(),
            'conc_value': conc_value,
            'conc_unit': conc_unit,
            'food_part': None,
            'food_processing': None,
            'source': 'fdc',
            'reference': "https://fdc.nal.usda.gov/fdc-app.html#/food-details/"
                f"{fdc_id}/nutrients",
            'quality_score': None,
            '_extracted_conc': None,
            '_extracted_food_part': None,
        }
        for head_id, tail_id in product(lut_food[key_food], lut_chem[key_chem]):
            if (head_id, tail_id) not in triplets_ht:
                triplets_ht[(head_id, tail_id)] = f"t{foodatlas_tid_curr}"
                triplets += [{
                    'foodatlas_id': triplets_ht[(head_id, tail_id)],
                    'head_id': head_id,
                    'relationship_id': 'r1',
                    'tail_id': tail_id,
                }]
                foodatlas_tid_curr += 1
            containing_['tids'] += [triplets_ht[(head_id, tail_id)]]
        containing += [containing_]
        foodatlas_mid_curr += 1

    foodatlas_tid_curr = 1
    foodatlas_mid_curr = 1
    triplets_ht = {}
    triplets = []
    containing = []
    food_nutrient.apply(_get_triplet, axis=1)
    triplets = pd.DataFrame(triplets)
    containing = pd.DataFrame(containing)

    triplets.to_csv('outputs/kg/triplets.tsv', sep='\t', index=False)
    containing.to_csv('outputs/kg/mdata_contains.tsv', sep='\t', index=False)

if __name__ == '__main__':
    get_triplets()

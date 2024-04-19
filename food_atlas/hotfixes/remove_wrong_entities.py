from ast import literal_eval

import pandas as pd


lut_food = pd.read_csv(
    "outputs/kg/lookup_table_food.tsv",
    sep='\t',
    converters={
        'foodatlas_id': literal_eval,
    }
)
lut_chemical = pd.read_csv(
    "outputs/kg/lookup_table_chemical.tsv",
    sep='\t',
    converters={
        'foodatlas_id': literal_eval,
    }
)

lut_food_dict = dict(
    zip(lut_food['name'], lut_food['foodatlas_id'])
)
lut_chemical_dict = dict(
    zip(lut_chemical['name'], lut_chemical['foodatlas_id'])
)

# n_found = 0
# for food_name in lut_food_dict:
#     if food_name in lut_chemical_dict:
#         print(f"Found food in chemical: {food_name}")
#         n_found += 1
# print(f"Found {n_found} matches")
# exit()
n_found = 0
for chemical_name in lut_chemical_dict:
    if chemical_name in lut_food_dict:
        print(f"Found chemical in food: {chemical_name}")
        n_found += 1
print(f"Found {n_found} matches")

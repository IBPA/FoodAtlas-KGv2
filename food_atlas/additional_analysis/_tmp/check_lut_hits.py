import pandas as pd

from ..kg.utils import load_lookup_tables


lut_food, lut_chemical = load_lookup_tables()
triplets = pd.read_csv("outputs/kg/20240214102617/triplets_cleaned.tsv", sep='\t')

foods = triplets['head'].unique()
chemicals = triplets['tail'].unique()

n_in_lut = 0
for food in foods:
    if food in lut_food:
        n_in_lut += 1
print(f"{n_in_lut}/{len(foods)} unique foods in LUT")

n_in_lut = 0
for chem in chemicals:
    if chem in lut_chemical:
        n_in_lut += 1
print(f"{n_in_lut}/{len(chemicals)} unique chemicals not in LUT")

# Check the performance after singularizing and pluralizing.

from inflection import singularize, pluralize

foods_ignored = []

n_in_lut = 0
for food in foods:
    food_candids = set([food, singularize(food), pluralize(food)])
    if len(food_candids) == 3:
        # Edge case: When a term ends with -sis, and this term is not common. For
        # example, scientific names like 'citrus sinensis' fall under here. Less than 1%
        # of the cases, and all of them so far are scientific names. Relatively safe to
        # ignore this case.
        foods_ignored += [food]
    # elif len(food_candids) == 2:
    #     # normal cases.
    #     pass
    # else:
    #     # Edge case: Uncountable nouns, such as 'fish', 'species'. Safely merge them
    #     # with their singular form.
    #     pass
    else:
        found = False
        for food_candid in food_candids:
            if food_candid in lut_food:
                found = True
                break
        n_in_lut += 1 if found else 0
# print(f"{len(foods_ignored)} foods ignored.")
print(
    f"{n_in_lut}/{len(foods)} unique foods not in LUT after singularizing and "
    "pluralizing."
)

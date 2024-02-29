import pandas as pd

from ..utils import load_lookup_tables, constants


if __name__ == '__main__':
    fdc_ids_ff = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/foundation_food.csv"
    )['fdc_id']

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
    food_nutrient = food_nutrient[food_nutrient['fdc_id'].isin(fdc_ids_ff)]
    food_nutrient = food_nutrient.query("nutrient_id != 2066").copy()
    food_nutrient['food_name'] = food.loc[food_nutrient['fdc_id'], 'description'].values
    food_nutrient['food_name'] = food_nutrient['food_name'].str.strip().str.lower()
    food_nutrient['chem_name'] \
        = nutrient.loc[food_nutrient['nutrient_id'], 'name'].values
    food_nutrient['chem_name'] = food_nutrient['chem_name'].str.strip().str.lower()

    lut_food, lut_chem = load_lookup_tables("outputs/kg/initialization")

    def _get_metadatum(row):
        global metadata_rows

        fdc_id = row['fdc_id']
        fdc_nutrient_id = row['nutrient_id']
        key_food = constants.get_lookup_key_by_id("fdc_ids", fdc_id)
        key_chem = constants.get_lookup_key_by_id("fdc_nutrient_ids", fdc_nutrient_id)
        if key_food not in lut_food:
            print(f"Food not found: {key_food}")
            return
        if key_chem not in lut_chem:
            print(f"Chemical not found: {key_chem}")
            return

        conc_value = row['amount']
        conc_unit = f"{nutrient.loc[fdc_nutrient_id, 'unit_name'].lower()}/100g"

        metadata_row = {
            'conc_value': None,
            'conc_unit': None,
            'food_part': None,
            'food_processing': None,
            'source': 'fdc',
            'reference': {
                'url': "https://fdc.nal.usda.gov/fdc-app.html#/food-details/"
                f"{fdc_id}/nutrients",
            },
            'quality_score': None,
            '_food_name': key_food,
            '_chemical_name': key_chem,
            '_conc': f'{conc_value} {conc_unit}',
            '_food_part': '',
        }

        metadata_rows += [metadata_row]

    metadata_rows = []
    food_nutrient.apply(_get_metadatum, axis=1)
    metadata = pd.DataFrame(metadata_rows)

    metadata.to_csv(
        "outputs/kg/merge_dbs/fdc/_metadata_new.tsv",
        sep='\t',
        index=False,
    )

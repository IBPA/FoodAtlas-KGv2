from ast import literal_eval

import pandas as pd


if __name__ == '__main__':
    cdno = pd.read_csv(
        "outputs/data_processing/cdno_cleaned.tsv",
        sep='\t',
        converters={'fdc_nutrient_ids': literal_eval},
    )
    fdc_ids = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18/food_nutrient.csv",
    )['nutrient_id'].tolist()
    fdc_ids = set(fdc_ids) - {2066}
    print(len(fdc_ids))

    fdc_nutrients = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18/nutrient.csv"
    ).set_index('id')

    fdc_ids_cdno = set()
    for fdc_id in cdno['fdc_nutrient_ids']:
        fdc_ids_cdno.update([int(x) for x in fdc_id])
    print(len(fdc_ids_cdno))

    print(len(fdc_ids_cdno & fdc_ids))

    # print(cdno)
    # print(fdc)

    print(fdc_nutrients.loc[list(fdc_ids - fdc_ids_cdno)])

    fdc = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18/food_nutrient.csv",
    )
    print(fdc.query(f"nutrient_id.isin({list(fdc_ids - fdc_ids_cdno)})"))

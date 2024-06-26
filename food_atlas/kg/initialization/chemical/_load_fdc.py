import pandas as pd


def load_fdc_nutrient() -> pd.DataFrame:
    """Load the FoodData Central nutrient data.

    Returns:
        pd.DataFrame: FDC nutrient data.

    """
    fdc = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18/nutrient.csv"
    )
    fdc['name'] = fdc['name'].str.lower().str.strip()

    # Manual correction: 4 entities of 'energy' with different derivations. Only keep
    # the most popular one: FDC Nutrient ID 2047 - energy (atwater general factors).
    ids_remove = [2048, 1008, 1062]
    fdc = fdc[~fdc['id'].isin(ids_remove)].copy()
    fdc.at[fdc[fdc['id'] == 2047].index[0], 'name'] = 'energy'

    return fdc.set_index('id')

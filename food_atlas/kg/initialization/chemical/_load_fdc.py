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

    return fdc.set_index('id')

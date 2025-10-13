# -*- coding: utf-8 -*-
"""Load FDC entries.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""

import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

tqdm.pandas()
pandarallel.initialize(progress_bar=True)

PATH_FDC_DATA_DIR = "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18"


def load_fdc(path_data_dir: str = PATH_FDC_DATA_DIR) -> pd.DataFrame:
    """Load and process the food entities in FDC.

    Args:
        path_data_dir (str): The path to the directory containing FDC data.

    Returns:
        pd.DataFrame: The food entities in FDC.

    """
    fdc_ids_ff = pd.read_csv(f"{path_data_dir}/foundation_food.csv")["fdc_id"]
    foods = pd.read_csv(
        f"{path_data_dir}/food.csv",
        usecols=["fdc_id", "description"],
    ).set_index("fdc_id")
    foods = foods[foods.index.isin(fdc_ids_ff)]
    foods["description"] = foods["description"].str.strip().str.lower()

    # Get FoodOn IDs.
    food_attr = pd.read_csv(
        f"{path_data_dir}/food_attribute.csv",
    )
    food_attr = food_attr[food_attr["fdc_id"].isin(fdc_ids_ff)]
    food_attr = food_attr.set_index("fdc_id")

    def extract_urls(row):
        attr_name = "FoodOn Ontology ID for FDC Item"

        # Targeted fixes: Deal with FDC items without FoodOn IDs.
        if row.name not in food_attr.index:
            if row.name == 2512381:
                foodon_url = "http://purl.obolibrary.org/obo/FOODON_03000273"
            else:
                raise ValueError(f"FDC item without FoodOn ID exists: {row.name}")

            return pd.Series({"foodon_url": foodon_url})

        attr = food_attr.loc[row.name]
        if isinstance(attr, pd.Series):
            attr = pd.DataFrame(attr).T
        attr = attr.set_index("name")

        foodon_urls = attr.query(f"name == '{attr_name}'")["value"].unique().tolist()

        if len(foodon_urls) == 0:
            # This case should not happen. Raise just in case.
            raise ValueError("FDC item without FoodOn ID exists.")
        elif len(foodon_urls) > 1:
            # Targeted fixes: Deal with multiple FoodOn URLs.
            if row.name == 323121:
                foodon_url = "http://purl.obolibrary.org/obo/FOODON_03310577"
            elif row.name == 330137:
                foodon_url = "http://purl.obolibrary.org/obo/FOODON_00004409"
            else:
                raise ValueError(f"Unaddressed multiple FoodOn URLs: {foodon_urls}")
        else:
            foodon_url = foodon_urls[0]

        return pd.Series({"foodon_url": foodon_url})

    foods = pd.concat(
        [foods, foods.apply(extract_urls, axis=1)],
        axis=1,
    )

    return foods[["description", "foodon_url"]]

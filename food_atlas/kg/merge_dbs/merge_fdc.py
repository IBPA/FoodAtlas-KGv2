# -*- coding: utf-8 -*-
"""

Merge FDC entries.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""

import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

from ...tests.unit_test_kg import test_all
from .. import KnowledgeGraph
from ..preprocessing import standardize_chemical_conc

tqdm.pandas()
pandarallel.initialize(progress_bar=True)

PATH_FDC_DATA_DIR = "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18"


def load_mapper_fdc_nutrient_id_to_foodatlas_id(entities: pd.DataFrame) -> dict:
    """Load the mapping from FDC nutrient IDs to FoodAtlas IDs.

    Args:
        entities (pd.DataFrame): The entities in the knowledge graph.

    Returns:
        dict: The mapping from FDC nutrient IDs to FoodAtlas IDs.

    """

    def add_to_mapper(row):
        nonlocal fdcn2fa

        if "fdc_nutrient" in row["external_ids"]:
            fdcn_ids = row["external_ids"]["fdc_nutrient"]
            for fdcn_id in fdcn_ids:
                if fdcn_id in fdcn2fa:
                    raise ValueError(f"Duplicate FDC nutrient ID: {fdcn_id}")
                fdcn2fa[fdcn_id] = row.name

    fdcn2fa = {}
    entities.apply(add_to_mapper, axis=1)

    return fdcn2fa


def load_mapper_fdc_id_to_foodatlas_id(entities: pd.DataFrame) -> dict:
    """Load the mapping from FDC IDs to FoodAtlas IDs.

    Args:
        entities (pd.DataFrame): The entities in the knowledge graph.

    Returns:
        dict: The mapping from FDC IDs to FoodAtlas IDs.

    """

    def add_to_mapper(row):
        nonlocal fdc2fa

        if "fdc" in row["external_ids"]:
            fdc_ids = row["external_ids"]["fdc"]
            for fdc_id in fdc_ids:
                if fdc_id in fdc2fa:
                    raise ValueError(f"Duplicate FDC ID: {fdc_id}")
                fdc2fa[fdc_id] = row.name

    fdc2fa = {}
    entities.apply(add_to_mapper, axis=1)

    return fdc2fa


def load_fdc(path_data_dir: str = PATH_FDC_DATA_DIR) -> pd.DataFrame:
    """Load and process the content of FDC.

    Args:
        path_data_dir (str): The path to the directory containing FDC data.

    Returns:
        pd.DataFrame: The content of FDC.

    """
    # Load relevant files.
    food_nutrient = pd.read_csv(
        f"{path_data_dir}/food_nutrient.csv",
        usecols=["id", "fdc_id", "nutrient_id", "amount"],
    )
    fdc_ids_ff = pd.read_csv(f"{path_data_dir}/foundation_food.csv")["fdc_id"]

    # Get chemical concentrations.
    food_nutrient = food_nutrient[food_nutrient["fdc_id"].isin(fdc_ids_ff)]

    return food_nutrient


if __name__ == "__main__":
    kg = KnowledgeGraph()
    entities = kg.entities._entities
    lut_food = kg.entities._lut_food

    fdc2fa = load_mapper_fdc_id_to_foodatlas_id(entities)
    fdcn2fa = load_mapper_fdc_nutrient_id_to_foodatlas_id(entities)

    fdc = load_fdc(PATH_FDC_DATA_DIR)
    fdc_nutrients = pd.read_csv(
        f"{PATH_FDC_DATA_DIR}/nutrient.csv",
        usecols=["id", "name", "unit_name"],
    ).set_index("id")

    # Clean FDC data.
    fdc = fdc[(fdc["fdc_id"].isin(fdc2fa)) & (fdc["nutrient_id"].isin(fdcn2fa))]
    print(fdc)

    # Create metadata.
    def _get_metadatum(row):
        global metadata_rows

        fdc_id = int(row["fdc_id"])
        fdc_nutrient_id = int(row["nutrient_id"])
        _food_name = f"FDC:{fdc_id}"
        _chemical_name = f"FDC_NUTRIENT:{fdc_nutrient_id}"
        conc_value = row["amount"]
        conc_unit = f"{fdc_nutrients.loc[fdc_nutrient_id, 'unit_name'].lower()}/100g"
        metadata_rows += [
            {
                "foodatlas_id": f"{kg.metadata.FAID_PREFIX}{kg.metadata._curr_mcid}",
                "conc_value": None,
                "conc_unit": None,
                "food_part": None,
                "food_processing": None,
                "source": "fdc",
                "reference": {
                    "url": "https://fdc.nal.usda.gov/fdc-app.html#/food-details/"
                    f"{fdc_id}/nutrients",
                },
                "entity_linking_method": "id_matching",
                "quality_score": None,
                "_food_name": _food_name,
                "_chemical_name": _chemical_name,
                "_conc": f"{conc_value} {conc_unit}",
                "_food_part": "",
            }
        ]
        kg.metadata._curr_mcid += 1

    metadata_rows = []
    fdc.progress_apply(_get_metadatum, axis=1)
    metadata = pd.DataFrame(metadata_rows)
    metadata = standardize_chemical_conc(metadata)

    # Add triplets.
    triplets_new_ht = {}
    for _, row in metadata.iterrows():
        head_id = fdc2fa[int(row["_food_name"].split(":")[-1])]
        tail_id = fdcn2fa[int(row["_chemical_name"].split(":")[-1])]
        if f"{head_id}-{tail_id}" in triplets_new_ht:
            triplets_new_ht[f"{head_id}-{tail_id}"]["metadata_ids"] += [
                row["foodatlas_id"]
            ]
        else:
            triplets_new_ht[f"{head_id}-{tail_id}"] = {
                "foodatlas_id": f"{kg.triplets.FAID_PREFIX}{kg.triplets._curr_tid}",
                "head_id": head_id,
                "relationship_id": "r1",
                "tail_id": tail_id,
                "metadata_ids": [row["foodatlas_id"]],
            }
            kg.triplets._curr_tid += 1

    triplets_new = pd.DataFrame(triplets_new_ht.values())

    # Add to KG.
    metadata = pd.concat([kg.metadata._metadata_contains.reset_index(), metadata])
    kg.metadata._metadata_contains = metadata[kg.metadata.COLUMNS].set_index(
        "foodatlas_id"
    )
    triplets = pd.concat([kg.triplets._triplets.reset_index(), triplets_new])
    kg.triplets._triplets = triplets[kg.triplets.COLUMNS].set_index("foodatlas_id")

    test_all(kg)
    kg.save("outputs/kg")

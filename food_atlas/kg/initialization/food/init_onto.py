# -*- coding: utf-8 -*-
"""

Initialize food triplets based on FoodOn.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""

from ast import literal_eval

import pandas as pd

from ._load_foodon import load_foodon

if __name__ == "__main__":
    foodon = load_foodon()
    foodon_food = foodon[foodon["is_food"]]
    entities = pd.read_csv(
        "outputs/kg/entities.tsv", sep="\t", converters={"external_ids": literal_eval}
    ).set_index("foodatlas_id")

    foodon2fa = {}
    for faid, row in entities.iterrows():
        if "foodon" not in row["external_ids"]:
            continue
        foodon2fa[row["external_ids"]["foodon"][0]] = faid

    foodon_ids = foodon_food.index.tolist()

    # Traverse the FoodOn hierarchy to generate is_a triplets
    food_ontology_rows = []
    visited = set()
    curr_miid = 1
    for foodon_id in foodon_ids:
        queue = [foodon_id]
        while queue:
            current = queue.pop()
            if current in visited:
                continue

            visited.add(current)
            for parent in foodon_food.loc[current, "parents"]:
                if parent in foodon_food.index:
                    queue.append(parent)
                    food_ontology_rows += [
                        {
                            "foodatlas_id": None,
                            "head_id": foodon2fa[current],
                            "relationship_id": "r2",
                            "tail_id": foodon2fa[parent],
                            "source": "foodon",
                        }
                    ]

    food_ontology = pd.DataFrame(food_ontology_rows)
    food_ontology["foodatlas_id"] = [
        f"fo{i}" for i in list(range(1, 1 + len(food_ontology)))
    ]

    food_ontology.to_csv("outputs/kg/food_ontology.tsv", sep="\t", index=False)

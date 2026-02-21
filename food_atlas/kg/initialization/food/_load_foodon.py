# -*- coding: utf-8 -*-
"""

Load functions for FoodOn.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""

from ast import literal_eval

import pandas as pd
from inflection import pluralize, singularize


def load_foodon():
    """Load the FoodOn ontology if exists. Otherwise, clean the raw FoodOn synonyms.

    Returns:
        pd.DataFrame: The FoodOn ontology.

    """
    foodon = pd.read_csv(
        "outputs/data_processing/foodon_cleaned.tsv",
        sep="\t",
        converters={
            "parents": literal_eval,
            "synonyms": literal_eval,
            "derives_from": literal_eval,
            "in_taxon": literal_eval,
            "derives": literal_eval,
            "has_part": literal_eval,
        },
    ).set_index("foodon_id")

    return foodon


def load_lut_food(
    resolve_organisms=True,
    resolve_singular_plural_forms=True,
) -> dict:
    """Load the lookup table for food entities.

    Args:
        resolve_organisms (bool): Whether to resolve organisms.
        resolve_singular_plural_forms (bool): Whether to resolve singular and plural
            forms.

    Returns:
        dict: The lookup table for food entities.

    """
    foodon = load_foodon()

    # Create a lookup table.
    def _update_lut_for_food(row, level):
        nonlocal lut_food

        if not row["is_food"]:
            return

        for syn in row["synonyms"][level]:
            syn = syn.lower()
            if syn not in lut_food:
                lut_food[syn] = row.name

    lut_food = {}
    for syn_level in [
        "label",
        "label (alternative)",
        "synonym (exact)",
        "synonym",
        "synonym (narrow)",
        "synonym (broad)",
    ]:
        foodon.apply(
            lambda row, syn_level=syn_level: _update_lut_for_food(row, syn_level),
            axis=1
        )

    if resolve_organisms:
        # Resolve organisms.
        def _update_lut_for_organism(row, level):
            nonlocal lut_food

            if row["is_food"]:
                return

            candidates = list(set(row["derives"] + row["has_part"]))
            if len(candidates) == 1:
                for syn in row["synonyms"][level]:
                    syn = syn.lower()
                    if syn not in lut_food:
                        lut_food[syn] = candidates[0]
            elif len(candidates) > 1:
                return candidates

        for syn_level in [
            "label",
            "label (alternative)",
            "synonym (exact)",
            "synonym",
            "synonym (narrow)",
            "synonym (broad)",
        ]:
            foodon.apply(
                lambda row, syn_level=syn_level: _update_lut_for_organism(
                    row, syn_level
                ),
                axis=1,
            )

    if resolve_singular_plural_forms:
        lut_food_to_add = {}
        for name, foodon_id in lut_food.items():
            if not name[-1].isalpha():
                continue

            name_sin = singularize(name)
            name_plu = pluralize(name)

            if name_sin not in lut_food:
                if name_sin in lut_food_to_add:
                    raise ValueError(f"Duplicate singular form: {name_sin}")
                lut_food_to_add[name_sin] = foodon_id
            if name_plu not in lut_food:
                if name_plu in lut_food_to_add:
                    raise ValueError(f"Duplicate plural form: {name_plu}")
                lut_food_to_add[name_plu] = foodon_id

        lut_food = {**lut_food, **lut_food_to_add}

    return lut_food

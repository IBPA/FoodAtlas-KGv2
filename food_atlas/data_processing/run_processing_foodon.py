# -*- coding: utf-8 -*-
"""

Process and clean FoodOn.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

"""

import pandas as pd
from owlready2 import get_ontology
from pandarallel import pandarallel
from tqdm import tqdm

tqdm.pandas()
pandarallel.initialize(progress_bar=True)


def _clean(foodon_synonyms: pd.DataFrame) -> pd.DataFrame:
    """Clean the raw FoodOn synonyms.

    Args:
        foodon_synonyms (pd.DataFrame): The raw FoodOn synonyms.

    Returns:
        pd.DataFrame: The cleaned FoodOn synonyms.

    """

    def _remove_brackets(x):
        if pd.isna(x):
            return x
        if x.startswith("<") and x.endswith(">"):
            return x[1:-1]
        else:
            return x

    def _remove_suffix(x):
        if "@" in x:
            return x.split("@")[0]
        if "^^" in x:
            return x.split("^^")[0]
        return x

    def _parse_entity(group):
        parents = group["?parent"].dropna().tolist()
        synonyms = {
            "label": [],
            "label (alternative)": [],
            "synonym (exact)": [],
            "synonym": [],
            "synonym (narrow)": [],
            "synonym (broad)": [],
            "taxon": [],
        }
        group.dropna(subset=["?type"]).apply(
            lambda row: synonyms[row["?type"]].append(_remove_suffix(row["?label"])),
            axis=1,
        )

        row = {
            "parents": parents,
            "synonyms": synonyms,
        }

        return row

    foodon_synonyms["?class"] = foodon_synonyms["?class"].apply(_remove_brackets)
    foodon_synonyms["?parent"] = foodon_synonyms["?parent"].apply(_remove_brackets)
    entities = (
        foodon_synonyms.groupby("?class").parallel_apply(_parse_entity).apply(pd.Series)
    )
    entities.index.name = "foodon_id"

    return entities


def _label_is_food(foodon: pd.DataFrame) -> pd.DataFrame:
    """Label FoodOn entry as food if has an ancestor of `food product by organism`.

    Args:
        foodon (pd.DataFrame): The FoodOn entries.

    Returns:
        pd.DataFrame: The FoodOn entries with the `is_food` label.

    """
    root = "http://www.w3.org/2002/07/owl#Thing"
    food = "http://purl.obolibrary.org/obo/FOODON_00002381"  # food by organism
    visited = {}
    visited[food] = True

    def _is_food(row):
        """Deapth-first search to label FoodOn entry as food."""

        nonlocal visited

        def dfs(foodon_id):
            if foodon_id in visited:
                return visited[foodon_id]
            if foodon_id not in foodon.index:
                return False

            if foodon_id == root:
                return False
            elif foodon_id == food:
                return True
            else:
                results = []
                for parent in foodon.loc[foodon_id, "parents"]:
                    results += [dfs(parent)]

                if any(results):
                    visited[foodon_id] = True
                    return True
                else:
                    visited[foodon_id] = False
                    return False

        dfs(row.name)

    foodon.progress_apply(_is_food, axis=1)
    foodon["is_food"] = foodon.index.map(visited)

    return foodon


def _label_is_organism(foodon: pd.DataFrame) -> pd.DataFrame:
    """Label FoodOn entry as organism if has an ancestor of `organism`.

    Args:
        foodon (pd.DataFrame): The FoodOn entries.

    Returns:
        pd.DataFrame: The FoodOn entries with the `is_organism` label.

    """
    root = "http://www.w3.org/2002/07/owl#Thing"
    organism = "http://purl.obolibrary.org/obo/OBI_0100026"

    visited = {}
    visited[organism] = True

    def _is_organism(row):
        """Deapth-first search to label FoodOn entry as organism."""

        nonlocal visited

        def dfs(foodon_id):
            if foodon_id in visited:
                return visited[foodon_id]
            if foodon_id not in foodon.index:
                return False

            if foodon_id == root:
                return False
            elif foodon_id == organism:
                return True
            else:
                results = []
                for parent in foodon.loc[foodon_id, "parents"]:
                    results += [dfs(parent)]

                if any(results):
                    visited[foodon_id] = True
                    return True
                else:
                    visited[foodon_id] = False
                    return False

        dfs(row.name)

    foodon.progress_apply(_is_organism, axis=1)
    foodon["is_organism"] = foodon.index.map(visited)

    return foodon


def _append_additional_relationships(foodon: pd.DataFrame) -> pd.DataFrame:
    """Derive additional relationships for convenience of downstream tasks.

    Args:
        foodon (pd.DataFrame): The FoodOn entries.

    Returns:
        pd.DataFrame: The FoodOn entries with additional relationships.

    """

    def _parse_derives_from_relationship(row):
        def _rename_foodon_id(foodon_id: str):
            # "obo.NCBITaxon_7460" => "<http://purl.obolibrary.org/obo/NCBITaxon_7460>"
            if foodon_id.startswith("obo."):
                return f"http://purl.obolibrary.org/obo/{foodon_id[4:]}"
            else:
                raise ValueError(f"Unknown foodon_id: {foodon_id}")

        result = {
            "derives_from": [],
            "in_taxon": [],
        }

        foodon_id = row.name.split("/")[-1]
        entity = obo[str(foodon_id)]
        relationships = entity.is_a

        for relationship in relationships:
            if not hasattr(relationship, "property"):
                continue

            if relationship.property in [obo["RO_0001000"], obo["RO_0002162"]]:
                property_ = relationship.property
                relationships_ = [str(relationship.value)]

                if " | " in relationships_[0]:
                    relationships_ = relationships_[0].split(" | ")

                if " " in str(relationship.value):
                    continue

                relationships_ = list(map(_rename_foodon_id, relationships_))
                if property_ == obo["RO_0001000"]:
                    result["derives_from"] += relationships_
                elif property_ == obo["RO_0002162"]:
                    result["in_taxon"] += relationships_

        return result

    onto = get_ontology("data/FoodOn/foodon.owl").load()
    obo = onto.get_namespace("http://purl.obolibrary.org/obo/")

    foodon[["derives_from", "in_taxon"]] = foodon.progress_apply(
        _parse_derives_from_relationship,
        axis=1,
    ).apply(pd.Series)

    # Reverse "derives_from" relationships.
    def _traverse_derives_from_relationship(row):
        nonlocal derives

        if not row["is_food"]:
            return

        derives_from = row["derives_from"]
        for e in derives_from:
            if e not in derives:
                derives[e] = []
            derives[e].append(row.name)

    derives = {}
    foodon.progress_apply(_traverse_derives_from_relationship, axis=1)
    foodon["derives"] = foodon.index.map(
        lambda x: derives.get(x, []),
    )

    # Reverse "in_taxon" relationships.
    def _traverse_in_taxon_relationship(row):
        nonlocal has_part

        if not row["is_food"]:
            return

        in_taxon = row["in_taxon"]
        for e in in_taxon:
            if e not in has_part:
                has_part[e] = []
            has_part[e].append(row.name)

    has_part = {}
    foodon.progress_apply(_traverse_in_taxon_relationship, axis=1)
    foodon["has_part"] = foodon.index.map(
        lambda x: has_part.get(x, []),
    )

    return foodon


if __name__ == "__main__":
    foodon_synonyms = pd.read_csv("data/FoodOn/foodon-synonyms.tsv", sep="\t")
    foodon = _clean(foodon_synonyms)
    foodon = _label_is_food(foodon)
    foodon = _label_is_organism(foodon)
    foodon = foodon[foodon["is_food"] | foodon["is_organism"]]
    foodon = _append_additional_relationships(foodon)
    foodon.to_csv(
        "outputs/data_processing/foodon_cleaned.tsv",
        sep="\t",
    )

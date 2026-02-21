from ast import literal_eval

import pandas as pd


def load_entities():
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep="\t",
        converters={
            "synonyms": literal_eval,
            "external_ids": literal_eval,
            "_synonyms_display": literal_eval,
        },
    ).set_index("foodatlas_id")

    return entities


def load_metadata():
    metadata_contains = pd.read_csv(
        "outputs/kg/metadata_contains.tsv",
        sep="\t",
        converters={
            "reference": literal_eval,
            "_conc": lambda x: "" if pd.isna(x) else x,
            "_food_part": lambda x: "" if pd.isna(x) else x,
        },
    ).set_index("foodatlas_id")

    return metadata_contains


def load_triplets():
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep="\t",
        converters={"metadata_ids": literal_eval},
    ).set_index("foodatlas_id")

    return triplets

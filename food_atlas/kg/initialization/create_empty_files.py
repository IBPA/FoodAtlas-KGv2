import pandas as pd

from food_atlas.kg._metadata import Metadata
from food_atlas.kg._triplets import Triplets

from ..entities import Entities


def create_empty_files():
    # Empty entities file.
    pd.DataFrame(
        [],
        columns=Entities.COLUMNS,
    ).to_csv(
        "outputs/kg/entities.tsv",
        sep="\t",
        index=False,
    )

    pd.DataFrame(
        [
            {"foodatlas_id": "r1", "name": "contains"},
            {"foodatlas_id": "r2", "name": "is_a"},
            {"foodatlas_id": "r3", "name": "positively_correlates_with"},
            {"foodatlas_id": "r4", "name": "negatively_correlates_with"},
            {"foodatlas_id": "r5", "name": "has_flavor"},
        ],
    ).to_csv(
        "outputs/kg/relationships.tsv",
        sep="\t",
        index=False,
    )

    # Empty lookup table file for food.
    pd.DataFrame(
        [],
        columns=[
            "name",
            "foodatlas_id",
        ],
    ).to_csv(
        "outputs/kg/lookup_table_food.tsv",
        sep="\t",
        index=False,
    )

    # Empty lookup table file for chemical.
    pd.DataFrame(
        [],
        columns=[
            "name",
            "foodatlas_id",
        ],
    ).to_csv(
        "outputs/kg/lookup_table_chemical.tsv",
        sep="\t",
        index=False,
    )

    # Empty triplets file.
    pd.DataFrame(
        [],
        columns=Triplets.COLUMNS,
    ).to_csv(
        "outputs/kg/triplets.tsv",
        sep="\t",
        index=False,
    )

    # Empty metadata file.
    pd.DataFrame(
        [],
        columns=Metadata.COLUMNS,
    ).to_csv(
        "outputs/kg/metadata_contains.tsv",
        sep="\t",
        index=False,
    )

    # Empty retired file.
    pd.DataFrame(
        [],
        columns=[
            "foodatlas_id",
            "action",
            "destination",
        ],
    ).to_csv(
        "outputs/kg/retired.tsv",
        sep="\t",
        index=False,
    )


if __name__ == "__main__":
    create_empty_files()

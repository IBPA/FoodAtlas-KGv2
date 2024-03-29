import pandas as pd

from food_atlas.kg._entities import Entities
from food_atlas.kg._triplets import Triplets
from food_atlas.kg._metadata import Metadata


def create_empty_files():
    # Empty entities file.
    pd.DataFrame(
        [], columns=Entities.COLUMNS,
    ).to_csv(
        "outputs/kg/initialization/entities.tsv",
        sep='\t',
        index=False,
    )

    # Empty lookup table file for food.
    pd.DataFrame(
        [],
        columns=[
            'name',
            'foodatlas_id',
        ]
    ).to_csv(
        "outputs/kg/initialization/lookup_table_food.tsv",
        sep='\t',
        index=False,
    )

    # Empty lookup table file for chemical.
    pd.DataFrame(
        [],
        columns=[
            'name',
            'foodatlas_id',
        ]
    ).to_csv(
        "outputs/kg/initialization/lookup_table_chemical.tsv",
        sep='\t',
        index=False,
    )

    # Empty triplets file.
    pd.DataFrame(
        [], columns=Triplets.COLUMNS,
    ).to_csv(
        "outputs/kg/initialization/triplets.tsv",
        sep='\t',
        index=False,
    )

    # Empty metadata file.
    pd.DataFrame(
        [], columns=Metadata.COLUMNS,
    ).to_csv(
        "outputs/kg/initialization/metadata_contains.tsv",
        sep='\t',
        index=False,
    )

    # Empty retired file.
    pd.DataFrame(
        [],
        columns=[
            'foodatlas_id',
            'action',
            'destination',
        ]
    ).to_csv(
        "outputs/kg/initialization/retired.tsv",
        sep='\t',
        index=False,
    )


if __name__ == '__main__':
    create_empty_files()

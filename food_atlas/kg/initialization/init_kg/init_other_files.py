import pandas as pd


if __name__ == '__main__':
    # Empty triplet file.
    pd.DataFrame(
        [],
        columns=[
            'foodatlas_id', 'head_id', 'relationship_id', 'tail_id', 'metadata_ids',
        ]
    ).to_csv(
        "outputs/kg/initialization/triplets.tsv",
        sep='\t',
        index=False
    )

    # Empty metadata file.
    pd.DataFrame(
        [],
        columns=[
            'foodatlas_id', 'conc_value', 'conc_unit', 'food_part', 'food_processing',
            'source', 'reference', 'quality_score',
            '_food_name', '_chemical_name', '_conc', '_food_part',
        ]
    ).to_csv(
        "outputs/kg/initialization/metadata_contains.tsv",
        sep='\t',
        index=False
    )

    # Empty retired file.
    pd.DataFrame(
        [],
        columns=[
            'foodatlas_id', 'action', 'timestamp',
        ]
    ).to_csv(
        "outputs/kg/initialization/retired.tsv",
        sep='\t',
        index=False
    )

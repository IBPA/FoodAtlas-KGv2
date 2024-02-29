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
            'foodatlas_id', 'food_name', 'chemical_name', 'conc_value', 'conc_unit',
            'food_part', 'food_processing', 'source', 'reference', 'quality_score',
            '_extracted_conc', '_extracted_food_part',
        ]
    ).to_csv(
        "outputs/kg/initialization/mdata_contains.tsv",
        sep='\t',
        index=False
    )
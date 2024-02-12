from ast import literal_eval

import pandas as pd


def load_lookup_tables(
    path_dir: str = "outputs/kg"
) -> list[dict]:
    """
    """
    luts = []
    for suffix in ['food', 'chemical']:
        lut_df = pd.read_csv(
            f"{path_dir}/lookup_table_{suffix}.tsv",
            sep='\t',
            converters={'foodatlas_id': literal_eval},
        )
        lut = dict(zip(lut_df['name'], lut_df['foodatlas_id']))
        luts += [lut]

    return luts


def load_mdata(
    path_dir: str = "outputs/kg"
) -> list[pd.DataFrame]:
    """
    """
    mdata_dfs = []
    for suffux in ['contains']:
        mdata_dfs += [pd.read_csv(
            f"{path_dir}/mdata_{suffux}.tsv",
            sep='\t',
            converters={
                'tids': literal_eval,
                '_extracted_conc': lambda x: '' if pd.isna(x) else x,
                '_extracted_food_part': lambda x: '' if pd.isna(x) else x,
            },
        )]

    return mdata_dfs

def load_entities(
    path_file: str = "outputs/kg/entities.tsv"
) -> pd.DataFrame:
    """
    """
    return pd.read_csv(
        path_file,
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    )

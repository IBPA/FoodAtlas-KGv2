from ast import literal_eval

import pandas as pd


def load_cdno() -> pd.DataFrame:
    """Load CDNO data.

    Returns:
        pd.DataFrame: CDNO data.

    """
    cdno = pd.read_csv(
        "outputs/data_processing/cdno_cleaned.tsv",
        sep='\t',
        converters={
            'fdc_nutrient_ids': literal_eval,
        }
    )

    # Further clean the data.
    cdno = cdno.rename(columns={'index': 'cdno_id'})
    cdno['cdno_id'] = cdno['cdno_id'].apply(lambda x: x.split('/')[-1])
    cdno['chebi_id'] = cdno['chebi_id'].apply(
        lambda x: int(x.split('/')[-1].split('_')[-1]) if isinstance(x, str) else None
    ).astype('Int64')
    cdno['fdc_nutrient_ids'] = cdno['fdc_nutrient_ids'].apply(
        lambda x: [int(xx) for xx in x]
    )
    cdno['label'] = cdno['label'].apply(
        lambda x: x.split('concentration of ')[-1].split(' in material entity')[0]
    )

    # Manual correction: 'nitrogen atom' is ambiguous, assign to another ChEBI ID.
    cdno.loc[cdno['label'] == 'nitrogen atom', 'chebi_id'] = 29351

    return cdno[['cdno_id', 'label', 'chebi_id', 'fdc_nutrient_ids']].copy()

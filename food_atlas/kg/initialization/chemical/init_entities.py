import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

from ._load_chebi import (
    load_mapper_name_to_chebi_id,
    load_mapper_chebi_id_to_names,
)

tqdm.pandas()


def _get_entities_from_chebi() -> pd.DataFrame:
    """Prepare chemical entities from ChEBI. Each entity should have unique set of
    synonyms. Original ChEBI chemical entities with ambiguous synonyms were isolated
    and stored as placeholder entities.

    Returns:
        pd.DataFrame: Chemical entities.

    """
    # Unique entities from ChEBI.
    chebi2name = load_mapper_chebi_id_to_names()

    # Placeholder entities with ambiguous names.
    name2chebi = load_mapper_name_to_chebi_id()
    name2chebi_ph = name2chebi[name2chebi['CHEBI_ID'].apply(len) > 1].copy()
    name2chebi_ph['CHEBI_ID'] = name2chebi_ph['CHEBI_ID'].apply(lambda x: sorted(x))
    name2chebi_ph['_chebi_id_str'] = name2chebi_ph['CHEBI_ID'].apply(str)
    phs = name2chebi_ph.groupby('_chebi_id_str').progress_apply(
        lambda x: pd.Series({
            'CHEBI_ID': x['CHEBI_ID'].values[0],
            'NAME': x['NAME'].tolist(),
        })
    ).reset_index(drop=True)

    # Remove placeholder entity names from unique entity synonyms.
    chebi2name = chebi2name.set_index('CHEBI_ID')
    for _, row in phs.iterrows():
        ids = row['CHEBI_ID']
        for id_ in ids:
            for name in row['NAME']:
                chebi2name.at[id_, 'NAME'].remove(name)
    chebi2name = chebi2name.reset_index()

    entities_rows = []
    for _, row in chebi2name.iterrows():
        entities_rows.append({
            'foodatlas_id': None,
            'entity_type': 'chemical',
            'common_name': row['NAME'][0],
            'scientific_name': None,  # TODO: map to IUPAC name.
            'synonyms': row['NAME'],
            'external_ids': {
                'chebi': row['CHEBI_ID'],
            },
        })
    for _, row in phs.iterrows():
        entities_rows.append({
            'foodatlas_id': None,
            'entity_type': 'chemical',
            'common_name': row['NAME'][0],
            'scientific_name': None,
            'synonyms': row['NAME'],
            'external_ids': {
                'placeholder_to': [],
                '_chebi': row['CHEBI_ID'],
            },
        })
    entities = pd.DataFrame(entities_rows)

    return entities


def _get_entities_from_cdno():
    """"""
    pass


if __name__ == '__main__':
    # We initialize chemical entities by using ChEBI, CDNO, and FDC Nutrients. The
    # primary ID is ChEBI, followed by CDNO and FDC.
    entities_chebi = _get_entities_from_chebi()

    exit()
    mapper_name2chebi_id = load_mapper_name2chebi_id()
    mapper_name2chebi_id['_chebi_id'] = mapper_name2chebi_id['CHEBI_ID'].apply(
        lambda x: str(sorted(x))
    )

    def get_new_row(group):
        return pd.Series({
            'CHEBI_ID': group['CHEBI_ID'].values[0],
            'NAME': group['NAME'].tolist(),
        })
    chebi2names = mapper_name2chebi_id.groupby('_chebi_id').progress_apply(get_new_row)\
        .reset_index(drop=True)
    chebi2names = chebi2names.sort_values('CHEBI_ID', key=lambda x: len(x))

    print(chebi2names)
from ast import literal_eval

import pandas as pd
from tqdm import tqdm

tqdm.pandas()


if __name__ == '__main__':
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
        converters={
            'external_ids': literal_eval,
        },
    ).set_index('foodatlas_id')

    # ChEBI ontology.
    chebi_relationships = pd.read_csv(
        "data/ChEBI/relation.tsv",
        sep='\t',
        encoding='unicode_escape',
    )

    chebi2fa = {}
    for faid, row in entities.iterrows():
        if 'chebi' not in row['external_ids']:
            continue
        chebi2fa[row['external_ids']['chebi'][0]] = faid

    is_a_rows = []
    for _, row in chebi_relationships.iterrows():
        if row['TYPE'] != 'is_a':
            continue
        if not(row['INIT_ID'] in chebi2fa and row['FINAL_ID'] in chebi2fa):
            continue
        is_a_rows += [{
            'foodatlas_id': None,
            'head_id': chebi2fa[row['FINAL_ID']],
            'relationship_id': 'r2',
            'tail_id': chebi2fa[row['INIT_ID']],
            'source': 'chebi',
        }]
    is_a = pd.DataFrame(is_a_rows)

    # # CDNO ontology.
    # print(entities)

    is_a['foodatlas_id'] = [
        f"co{i}" for i in list(range(1, 1 + len(is_a)))
    ]
    is_a.to_csv(
        "outputs/kg/chemical_ontology.tsv",
        sep='\t',
        index=False,
    )

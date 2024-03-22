from ast import literal_eval

import pandas as pd

from ...kg._query import _fetch_ncbi_taxonomy

if __name__ == '__main__':
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    ).set_index('foodatlas_id')

    # # Food groups.
    # entities_food = entities[entities['entity_type'] == 'food']
    # groups_food = pd.DataFrame(
    #     [],
    #     columns=['ncbi_taxonomy_division'],
    #     index=entities_food.index,
    # )

    # # NCBI Taxonomy Division.
    # ncbi_taxon_ids = []
    # entities_food['external_ids'].apply(
    #     lambda x: ncbi_taxon_ids.append(x['ncbi_taxon_id'])
    #     if 'ncbi_taxon_id' in x else None,
    # )
    # records = _fetch_ncbi_taxonomy(ncbi_taxon_ids)
    # ncbi_taxon_id2division = dict(zip(records['TaxId'], records['Division']))

    # def _assign_ncbi_taxonomy_group(row):
    #     eid = row.name
    #     entity = entities.loc[eid]
    #     if 'ncbi_taxon_id' in entity['external_ids']:
    #         row['ncbi_taxonomy_division'] \
    #             = ncbi_taxon_id2division[entity['external_ids']['ncbi_taxon_id']]
    #     else:
    #         row['ncbi_taxonomy_division'] = 'Unclassified'

    #     return row

    # groups_food = groups_food.apply(_assign_ncbi_taxonomy_group, axis=1)
    # groups_food.to_csv(
    #     "outputs/kg/food_groups.tsv",
    #     sep='\t',
    # )

    entities_chemical = entities[entities['entity_type'] == 'chemical'].copy()
    entities_chemical = entities_chemical.loc[
        entities_chemical['scientific_name'].notna()
    ]
    print(entities_chemical)
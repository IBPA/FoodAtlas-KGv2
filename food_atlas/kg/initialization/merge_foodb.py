import pandas as pd

from ..utils import load_food_lookup_table, load_food_entities

if __name__ == '__main__':
    lut = load_food_lookup_table()
    entities = load_food_entities().set_index('foodatlas_id')

    data_foodb = pd.read_json("data/FooDB/Food.json", lines=True)
    data_foodb['ncbi_taxonomy_id'] = data_foodb['ncbi_taxonomy_id'].astype('Int64')

    # Merge FooDB foods to FoodAtlas IDs using NCBI Taxonomy IDs.
    def merge_foodb_by_ncbi_taxon_id(row):
        ncbi_taxon_id = row['ncbi_taxon_id']
        foodb_foods = data_foodb.query(
            f"ncbi_taxonomy_id == {ncbi_taxon_id}"
        )
        if len(foodb_foods) != 0:
            foodb_ids = foodb_foods['public_id'].tolist()
            foodb_names = foodb_foods['name'].str.lower().tolist()
            row['external_ids']['foodb_ids'] = foodb_ids
            for name in foodb_names:
                if name in lut and row.name not in lut[name]:
                    lut[name] += [row.name]
                    row['synonyms'] += [name]
                elif name not in lut:
                    lut[name] = [row.name]
                    row['synonyms'] += [name]

    entities['ncbi_taxon_id'] = entities['external_ids'].apply(
        lambda x: x['ncbi_taxon_id'] if 'ncbi_taxon_id' in x else None
    )
    entities.apply(merge_foodb_by_ncbi_taxon_id, axis=1)

    # Further merge if scientific name matched in the lookup table.
    def merge_foodb_by_scientific_name(row):
        global ids_unmerged
        global entities

        if row['name_scientific'] is None:
            ids_unmerged += [row.name]
            return

        x = row['name_scientific'].lower()
        x = x.replace('× ', 'x ').replace(' ssp. ', ' subsp. ')  # Reformat.
        x = x.replace(' l.', '').replace(' f. alba dc.', '')  # Remove authority.

        if x in lut:
            foodatalas_ids = lut[x]
            for foodatlas_id in foodatalas_ids:
                target_external_ids = entities.loc[foodatlas_id, 'external_ids']
                if 'foodb_ids' not in target_external_ids:
                    target_external_ids['foodb_ids'] = []
                target_external_ids['foodb_ids'] += [row['public_id']]
        else:
            ids_unmerged += [row.name]

    ids_unmerged = []
    data_foodb_rest = data_foodb.query(
        f"~ncbi_taxonomy_id.isin({entities['ncbi_taxon_id'].tolist()})"
    ).copy()
    data_foodb_rest.apply(merge_foodb_by_scientific_name, axis=1)

    # Further merge if common name matched in the lookup table.
    def merge_foodb_by_common_name(row):
        global entities
        global ids_unmerged

        if row['name'] is None:
            ids_unmerged += [row.name]
            return

        x = row['name'].lower()

        if x in lut:
            foodatalas_ids = lut[x]
            for foodatlas_id in foodatalas_ids:
                target_external_ids = entities.loc[foodatlas_id, 'external_ids']
                if 'foodb_ids' not in target_external_ids:
                    target_external_ids['foodb_ids'] = []
                target_external_ids['foodb_ids'] += [row['public_id']]
        else:
            ids_unmerged += [row.name]

    data_foodb_rest = data_foodb_rest.loc[ids_unmerged]
    ids_unmerged = []
    data_foodb_rest.apply(merge_foodb_by_common_name, axis=1)

    # If still remaining, add unmerged FooDB foods to the entities table.
    data_foodb_rest = data_foodb_rest.loc[ids_unmerged]

    def _merge_entity(row):
        global foodatlas_id_curr
        global entities_new_rows
        global lut

        entities_new_rows += [{
            'foodatlas_id': f"e{foodatlas_id_curr}",
            'entity_type': 'food',
            'common_name': row['name'].lower(),
            'scientific_name': '',
            'synonyms': [row['name'].lower()],
            'external_ids': {'foodb_id': [row['public_id']]},
            # 'food_category': 'UNCATEGORIZED',
        }]
        lut[row['name'].lower()] = [f"e{foodatlas_id_curr}"]
        foodatlas_id_curr += 1

    entities = entities.reset_index()
    foodatlas_id_curr = entities['foodatlas_id'].str.slice(1).astype(int).max() + 1
    entities_new_rows = []
    data_foodb_rest.apply(lambda row: _merge_entity(row), axis=1)

    entities_new = pd.DataFrame(entities_new_rows)
    entities = pd.concat([entities, entities_new], axis=0)
    entities = entities.drop(columns=['ncbi_taxon_id'])

    # Save the results.
    entities.to_csv("outputs/kg/entities.tsv", sep='\t', index=False)
    pd.DataFrame(lut.items(), columns=['name', 'foodatlas_id']).to_csv(
        "outputs/kg/food_lookup_table.tsv", sep='\t', index=False
    )

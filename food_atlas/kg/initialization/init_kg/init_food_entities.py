from ast import literal_eval
from collections import defaultdict

import pandas as pd


def create_entities_from_ncbi_taxonomy():
    data = pd.read_csv(
        "outputs/kg/initialization/ncbi_taxonomy.tsv",
        sep='\t',
        converters={
            'OtherNames': lambda x: literal_eval(x) if x != '' else None,
        },
    )

    def _parse_other_names(other_names):
        if other_names is None:
            return []

        synonyms = []
        synonyms += other_names['Synonym']
        synonyms += other_names['EquivalentName']
        synonyms += other_names['CommonName']
        synonyms += other_names['Includes']
        if 'GenbankCommonName' in other_names:
            synonyms += [other_names['GenbankCommonName']]
        if 'BlastName' in other_names:
            synonyms += [other_names['BlastName']]
        if other_names['Name']:
            for name in other_names['Name']:
                if name['ClassCDE'] == 'misspelling':
                    synonyms += [name['DispName']]
        synonyms = [synonym.strip().lower() for synonym in synonyms]

        return synonyms

    data['synonyms'] = data['OtherNames'].apply(_parse_other_names)
    data = data.rename(columns={
        'TaxId': 'ncbi_taxon_id',
        'ScientificName': 'scientific_name',
    })
    data['scientific_name'] = data['scientific_name'].str.strip().str.lower()
    data['synonyms'] = data.apply(
        lambda row: list(set(row['synonyms'] + [row['scientific_name']])),
        axis=1,
    )
    # Default common name is the shortest synonym. If there are no synonyms, the
    # common name is the scientific name.
    data['common_name'] = data.apply(
        lambda row: min(row['synonyms'], key=len),
        axis=1,
    )
    data['external_ids'] \
        = data['ncbi_taxon_id'].apply(lambda x: {'ncbi_taxon_id': x})
    data['foodatlas_id'] = [f"e{i}" for i in range(1, len(data) + 1)]
    data['entity_type'] = 'food'
    entities = data[[
        'foodatlas_id',
        'entity_type',
        'common_name',
        'scientific_name',
        'synonyms',
        'external_ids',
    ]]

    # Create the lookup table.
    lut = defaultdict(list)
    def _add_to_lut(row):
        nonlocal lut

        for name in row['synonyms']:
            lut[name] += [row['foodatlas_id']]
    entities.apply(_add_to_lut, axis=1)

    return entities, lut


def append_entities_from_foodb(entities, lut):
    entities = entities.set_index('foodatlas_id')
    data_foodb = pd.read_json("data/FooDB/Food.json", lines=True)
    data_foodb['ncbi_taxonomy_id'] = data_foodb['ncbi_taxonomy_id'].astype('Int64')

    # Merge FooDB foods to FoodAtlas IDs using NCBI Taxonomy IDs.
    def _merge_foodb_by_ncbi_taxon_id(row):
        ncbi_taxon_id = row['ncbi_taxon_id']
        foodb_foods = data_foodb.query(
            f"ncbi_taxonomy_id == {ncbi_taxon_id}"
        )
        if len(foodb_foods) != 0:
            foodb_ids = foodb_foods['public_id'].tolist()
            foodb_names = foodb_foods['name'].str.strip().str.lower().tolist()
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
    entities.apply(_merge_foodb_by_ncbi_taxon_id, axis=1)

    # Further merge if scientific name matched in the lookup table.
    def _merge_foodb_by_scientific_name(row):
        nonlocal ids_unmerged
        nonlocal entities

        if row['name_scientific'] is None:
            ids_unmerged += [row.name]
            return

        x = row['name_scientific'].strip().lower()
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
    data_foodb_rest.apply(_merge_foodb_by_scientific_name, axis=1)

    # Further merge if common name matched in the lookup table.
    def _merge_foodb_by_common_name(row):
        nonlocal entities
        nonlocal ids_unmerged

        if row['name'] is None:
            ids_unmerged += [row.name]
            return

        x = row['name'].strip().lower()

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
    data_foodb_rest.apply(_merge_foodb_by_common_name, axis=1)

    # If still remaining, add unmerged FooDB foods to the entities table.
    data_foodb_rest = data_foodb_rest.loc[ids_unmerged]

    def _merge_entity(row):
        nonlocal foodatlas_id_curr
        nonlocal entities_new_rows
        nonlocal lut

        entities_new_rows += [{
            'foodatlas_id': f"e{foodatlas_id_curr}",
            'entity_type': 'food',
            'common_name': row['name'].strip().lower(),
            'scientific_name': '',
            'synonyms': [row['name'].strip().lower()],
            'external_ids': {'foodb_ids': [row['public_id']]},
        }]
        lut[row['name'].strip().lower()] = [f"e{foodatlas_id_curr}"]
        foodatlas_id_curr += 1

    entities = entities.reset_index()
    foodatlas_id_curr = entities['foodatlas_id'].str.slice(1).astype(int).max() + 1
    entities_new_rows = []
    data_foodb_rest.apply(lambda row: _merge_entity(row), axis=1)

    entities_new = pd.DataFrame(entities_new_rows)
    entities = pd.concat([entities, entities_new], axis=0)
    entities = entities.drop(columns=['ncbi_taxon_id'])

    return entities, lut


def append_entities_from_fdc(entities, lut):

    def _check_fdc(data_fdc):
        fdc_cls = pd.read_csv("data/FDC/fdc_classification.tsv", sep='\t')

        ht = {}
        for col in fdc_cls.columns:
            for food in fdc_cls[col].dropna():
                ht[food.strip()] = 0

        for food in data_fdc['description']:
            if food not in ht:
                raise ValueError(
                    f"Food name {food} not found in the FDC classification."
                )
            else:
                ht[food] += 1

        for food, count in ht.items():
            if count != 1:
                raise ValueError(
                    f"Food name {food} found {count} times in the FDC classification."
                )

    def _load_foundation_food():
        PATH_DATA_DIR = "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26"

        foundation_food_ids \
            = pd.read_csv(f"{PATH_DATA_DIR}/foundation_food.csv")['fdc_id'].tolist()
        data_food = pd.read_csv(f"{PATH_DATA_DIR}/food.csv")
        data_food = data_food[data_food['fdc_id'].isin(foundation_food_ids)]
        data_attr = pd.read_csv(f"{PATH_DATA_DIR}/food_attribute.csv")
        data_attr = data_attr[data_attr['fdc_id'].isin(foundation_food_ids)]
        data_attr = data_attr.set_index('fdc_id')

        def extract_urls(row):
            result = {
                'ncbi_taxon_url': None,
            }
            if row['fdc_id'] in data_attr.index:
                attr = data_attr.loc[row['fdc_id']]
                if type(attr) == pd.Series:
                    attr = pd.DataFrame(attr).T
                attr = attr.set_index('name')

                result['ncbi_taxon_url'] = attr.loc['NCBI Taxon', 'value'] \
                    if 'NCBI Taxon' in attr.index else None

            return pd.Series(result)

        data_food = pd.concat(
            [data_food, data_food.apply(extract_urls, axis=1)],
            axis=1
        )

        # Parse NCBI Taxon ID from URL.
        def _parse_ncbi_taxon_url(url):
            if url is None:
                return None
            else:
                id_ = url.split('/')[-1]
                if id_.isdigit():
                    return int(id_)
                elif id_.startswith('NCBITaxon_'):
                    return int(id_.split('_')[-1])
                else:
                    return int(id_.split('=')[-1])

        data_food['ncbi_taxon_id'] \
            = data_food['ncbi_taxon_url'].apply(_parse_ncbi_taxon_url)
        data_food['ncbi_taxon_id'] = data_food['ncbi_taxon_id'].astype('Int64')
        data_food['description'] = data_food['description'].str.strip()

        _check_fdc(data_food)

        return data_food

    data_fdc = _load_foundation_food()
    fdc_cls = pd.read_csv("data/FDC/fdc_classification.tsv", sep='\t')

    entities = entities.set_index('foodatlas_id')
    entities['ncbi_taxon_id'] = entities['external_ids'].apply(
        lambda x: x['ncbi_taxon_id'] if 'ncbi_taxon_id' in x else None
    )
    entities['ncbi_taxon_id'] = entities['ncbi_taxon_id'].astype('Int64')

    # Merge FooDB non-many foods to FoodAtlas IDs using NCBI Taxonomy IDs.
    foods_many = fdc_cls['Processed many'].tolist()
    data_fdc_non_many = data_fdc.query(
        "ncbi_taxon_id.notna() "
        f"& (~description.isin({foods_many}))"
    )

    def _merge_fdc_by_ncbi_taxon_id(row):
        if pd.isna(row['ncbi_taxon_id']):
            return

        ncbi_taxon_id = row['ncbi_taxon_id']
        fdc_foods = data_fdc_non_many.query(
            f"ncbi_taxon_id == {ncbi_taxon_id}"
        )
        if len(fdc_foods) != 0:
            fdc_ids = fdc_foods['fdc_id'].tolist()
            fdc_names = fdc_foods['description'].str.strip().str.lower().tolist()
            row['external_ids']['fdc_ids'] = fdc_ids
            for name in fdc_names:
                if name in lut and row.name not in lut[name]:
                    lut[name] += [row.name]
                    row['synonyms'] += [name]
                elif name not in lut:
                    lut[name] = [row.name]
                    row['synonyms'] += [name]

    entities.apply(_merge_fdc_by_ncbi_taxon_id, axis=1)

    # Merge the rest of the foods by creating new nodes.
    data_fdc_rest = data_fdc.query(
        f"ncbi_taxon_id.isna() | description.isin({foods_many})"
    )

    # Sanity check: No food should be found in the lut.
    found_in_lut = []
    data_fdc_rest['description'].apply(
        lambda x: found_in_lut.append(x.strip().lower()) if x in lut else None
    )
    assert len(found_in_lut) == 0

    # Create new entities for the rest of the foods.
    def _merge_entity(row):
        nonlocal foodatlas_id_curr
        nonlocal entities_new_rows
        nonlocal lut

        entities_new_rows += [{
            'foodatlas_id': f"e{foodatlas_id_curr}",
            'entity_type': 'food',
            'common_name': row['description'].strip().lower(),
            'scientific_name': '',
            'synonyms': [row['description'].strip().lower()],
            'external_ids': {'fdc_ids': [row['fdc_id']]},
        }]
        lut[row['description'].strip().lower()] = [f"e{foodatlas_id_curr}"]
        foodatlas_id_curr += 1

    entities = entities.reset_index()
    foodatlas_id_curr = entities['foodatlas_id'].str.slice(1).astype(int).max() + 1
    entities_new_rows = []
    data_fdc_rest.apply(lambda row: _merge_entity(row), axis=1)

    entities_new = pd.DataFrame(entities_new_rows)
    entities = pd.concat([entities, entities_new], axis=0)
    entities = entities.drop(columns=['ncbi_taxon_id'])

    return entities, lut


if __name__ == '__main__':
    entities, lut = create_entities_from_ncbi_taxonomy()
    entities, lut = append_entities_from_foodb(entities, lut)
    entities, lut = append_entities_from_fdc(entities, lut)

    # Save the results.
    entities.to_csv("outputs/kg/initialization/entities.tsv", sep='\t', index=False)
    pd.DataFrame(lut.items(), columns=['name', 'foodatlas_id']).to_csv(
        "outputs/kg/initialization/lookup_table_food.tsv", sep='\t', index=False
    )

from ast import literal_eval
from collections import OrderedDict

import pandas as pd
from pandarallel import pandarallel
from tqdm import tqdm

from .. import KnowledgeGraph
from ..utils import constants
from ..preprocessing import standardize_chemical_conc
from ...tests.unit_test_kg import test_all

tqdm.pandas()
pandarallel.initialize(progress_bar=True)


# def _append_chemical_concentration(food_nutrient: pd.DataFrame) -> pd.DataFrame:
#     # Load relevant files.
#     fdc_ids_ff = pd.read_csv(
#         "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/foundation_food.csv"
#     )['fdc_id']
#     food = pd.read_csv(
#         "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food.csv",
#         usecols=['fdc_id', 'description'],
#     ).set_index('fdc_id')
#     nutrients = pd.read_csv(
#         "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/nutrient.csv",
#         usecols=['id', 'name', 'unit_name'],
#     ).set_index('id')

#     # Get chemical concentrations.
#     food_nutrient = food_nutrient[food_nutrient['fdc_id'].isin(fdc_ids_ff)]
#     food_nutrient = food_nutrient.query("nutrient_id != 2066").copy()
#     food_nutrient['food_name'] = food.loc[food_nutrient['fdc_id'], 'description'].values
#     food_nutrient['food_name'] = food_nutrient['food_name'].str.strip().str.lower()
#     food_nutrient['chem_name'] \
#         = nutrients.loc[food_nutrient['nutrient_id'], 'name'].values
#     food_nutrient['chem_name'] = food_nutrient['chem_name'].str.strip().str.lower()

#     return food_nutrient


# def _append_ncbi_taxonomy_id(food_nutrient: pd.DataFrame) -> pd.DataFrame:
#     # Load relevant files.
#     fdc_ids_ff = pd.read_csv(
#         "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/foundation_food.csv"
#     )['fdc_id']

#     food_attr = pd.read_csv(
#         "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food_attribute.csv",
#     )

#     # Get NCBI Taxon IDs.
#     food_attr = food_attr[food_attr['fdc_id'].isin(fdc_ids_ff)]
#     food_attr = food_attr.set_index('fdc_id')

#     def extract_urls(row):
#         result = {
#             'ncbi_taxon_url': None,
#         }
#         if row['fdc_id'] in food_attr.index:
#             attr = food_attr.loc[row['fdc_id']]
#             if type(attr) == pd.Series:
#                 attr = pd.DataFrame(attr).T
#             attr = attr.set_index('name')

#             result['ncbi_taxon_url'] = attr.loc['NCBI Taxon', 'value'] \
#                 if 'NCBI Taxon' in attr.index else None

#         return pd.Series(result)

#     food_nutrient = pd.concat(
#         [food_nutrient, food_nutrient.progress_apply(extract_urls, axis=1)],
#         axis=1,
#     )

#     # Parse NCBI Taxon ID from URL.
#     def parse_ncbi_taxon_url(url):
#         if url is None:
#             return None
#         else:
#             id_ = url.split('/')[-1]
#             if id_.isdigit():
#                 return int(id_)
#             elif id_.startswith('NCBITaxon_'):
#                 return int(id_.split('_')[-1])
#             else:
#                 return int(id_.split('=')[-1])

#     food_nutrient['ncbi_taxon_id'] = food_nutrient['ncbi_taxon_url'].progress_apply(
#         parse_ncbi_taxon_url
#     )
#     food_nutrient['ncbi_taxon_id'] = food_nutrient['ncbi_taxon_id'].astype('Int64')

#     # Remove NCBI Taxonomy IDs for processed many foods.
#     fdc_cls = pd.read_csv("data/FDC/fdc_classification.tsv", sep='\t')
#     foods_many = fdc_cls['Processed many'].str.lower().tolist()
#     food_nutrient.loc[
#         food_nutrient['food_name'].isin(foods_many), 'ncbi_taxon_id'
#     ] = None

#     return food_nutrient


# def _append_pubchem_cid(food_nutrient: pd.DataFrame) -> pd.DataFrame:
#     chemicals = pd.read_csv(
#         "outputs/kg/initialization/_pubchem_cids_fdc_manual.tsv", sep='\t',
#     )

#     def _parse_pubchem_cids(cids):
#         res = None

#         if pd.isna(cids):
#             res = []
#         elif cids.startswith('['):
#             res = literal_eval(cids)
#         else:
#             res = [int(cids)]

#         return res

#     chemicals['pubchem_cid'] = chemicals['pubchem_cid'].apply(_parse_pubchem_cids)
#     chemicals['name'] = chemicals['name'].str.strip().str.lower()
#     chemicals = chemicals.set_index('id')

#     food_nutrient['pubchem_cid'] = food_nutrient['nutrient_id'].progress_apply(
#         lambda x: chemicals.loc[x, 'pubchem_cid'] if x in chemicals.index else []
#     )

#     return food_nutrient

    # entities_chemical = entities.query("entity_type == 'chemical'").copy()
    # entities_chemical['pubchem_cid'] = entities_chemical['external_ids'].apply(
    #     lambda x: x['pubchem_cid']
    # )
    # entities_chemical = entities_chemical.set_index('pubchem_cid')
    # entities = entities.set_index('foodatlas_id')

    # def _merge_entities_by_pubchem_cid(row):
    #     nonlocal entities
    #     nonlocal lut

    #     for cid in row['pubchem_cid']:
    #         foodatlas_id = entities_chemical.loc[cid, 'foodatlas_id']
    #         entity = entities.loc[foodatlas_id]

    #         # Expand the external IDs.
    #         if 'fdc_nutrient_ids' not in entity['external_ids']:
    #             entity['external_ids']['fdc_nutrient_ids'] = []
    #         entity['external_ids']['fdc_nutrient_ids'] += [row['id']]

    #         # Expand the synonyms
    #         if row['name'] in entity['synonyms']:
    #             # This means the name is already in the lookup table.
    #             continue
    #         else:
    #             # Either the synonym in the lookup table but not linked to the entity,
    #             # or the synonym is not in the lookup table. In both cases, we need to
    #             # add the synonym to the entity and the lookup table.
    #             entity['synonyms'] += [row['name']]
    #             lut[row['name']] += [foodatlas_id]

    # chemicals[chemicals['pubchem_cid'].apply(lambda x: True if len(x) else False)].apply(
    #     _merge_entities_by_pubchem_cid, axis=1
    # )

    # def _expand_entities(row):
    #     nonlocal entities
    #     nonlocal lut
    #     nonlocal foodatlas_id_curr
    #     nonlocal entities_new_rows

    #     entities_new_rows += [{
    #         'foodatlas_id': f"e{foodatlas_id_curr}",
    #         'entity_type': 'chemical',
    #         'common_name': row['name'],
    #         'scientific_name': '',
    #         'synonyms': [row['name']],
    #         'external_ids': {'fdc_nutrient_ids': [row['id']]},
    #     }]
    #     lut[row['name']] += [f"e{foodatlas_id_curr}"]
    #     foodatlas_id_curr += 1

    # entities = entities.reset_index()
    # foodatlas_id_curr = entities['foodatlas_id'].str.slice(1).astype(int).max() + 1
    # entities_new_rows = []
    # chemicals[
    #     ~chemicals['pubchem_cid'].apply(lambda x: True if len(x) else False)
    # ].apply(
    #     _expand_entities, axis=1
    # )
    # entities = pd.concat([entities, pd.DataFrame(entities_new_rows)], ignore_index=True)


def _load_contains() -> pd.DataFrame:
    # Load relevant files.
    food_nutrient = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food_nutrient.csv",
        usecols=['id', 'fdc_id', 'nutrient_id', 'amount'],
    )
    fdc_ids_ff = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/foundation_food.csv"
    )['fdc_id']
    # food = pd.read_csv(
    #     "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food.csv",
    #     usecols=['fdc_id', 'description'],
    # ).set_index('fdc_id')
    # nutrients = pd.read_csv(
    #     "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/nutrient.csv",
    #     usecols=['id', 'name', 'unit_name'],
    # ).set_index('id')

    # Get chemical concentrations.
    food_nutrient = food_nutrient[food_nutrient['fdc_id'].isin(fdc_ids_ff)]
    food_nutrient = food_nutrient.query("nutrient_id != 2066").copy()
    # food_nutrient['food_name'] = food.loc[food_nutrient['fdc_id'], 'description'].values
    # food_nutrient['food_name'] = food_nutrient['food_name'].str.strip().str.lower()
    # food_nutrient['chem_name'] \
    #     = nutrients.loc[food_nutrient['nutrient_id'], 'name'].values
    # food_nutrient['chem_name'] = food_nutrient['chem_name'].str.strip().str.lower()

    return food_nutrient


def _load_foods():
    fdc_ids_ff = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/foundation_food.csv"
    )['fdc_id']
    foods = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food.csv",
        usecols=['fdc_id', 'description'],
    ).set_index('fdc_id')
    foods = foods[foods.index.isin(fdc_ids_ff)]
    foods['description'] = foods['description'].str.strip().str.lower()

    # Get NCBI Taxon IDs.
    food_attr = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/food_attribute.csv",
    )
    food_attr = food_attr[food_attr['fdc_id'].isin(fdc_ids_ff)]
    food_attr = food_attr.set_index('fdc_id')

    def extract_urls(row):
        result = {
            'ncbi_taxon_url': None,
        }
        if row.name in food_attr.index:
            attr = food_attr.loc[row.name]
            if type(attr) == pd.Series:
                attr = pd.DataFrame(attr).T
            attr = attr.set_index('name')

            result['ncbi_taxon_url'] = attr.loc['NCBI Taxon', 'value'] \
                if 'NCBI Taxon' in attr.index else None

        return pd.Series(result)

    foods = pd.concat(
        [foods, foods.apply(extract_urls, axis=1)],
        axis=1,
    )

    # Parse NCBI Taxon ID from URL.
    def parse_ncbi_taxon_url(url):
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

    foods['ncbi_taxon_id'] = foods['ncbi_taxon_url'].apply(parse_ncbi_taxon_url)
    foods['ncbi_taxon_id'] = foods['ncbi_taxon_id'].astype('Int64')

    # Remove NCBI Taxonomy IDs for processed many foods.
    fdc_cls = pd.read_csv("data/FDC/fdc_classification.tsv", sep='\t')
    foods_many = fdc_cls['Processed many'].str.lower().tolist()
    foods.loc[
        foods['description'].isin(foods_many), 'ncbi_taxon_id'
    ] = None

    def _parse_food_name(row):
        if pd.isna(row['ncbi_taxon_id']):
            return row['description']
        else:
            return constants.get_lookup_key_by_id('ncbi_taxon_id', row['ncbi_taxon_id'])

    foods['_food_name'] = foods.apply(_parse_food_name, axis=1)

    return foods[['description', 'ncbi_taxon_id', '_food_name']]


def _load_chemicals():
    chemicals = pd.read_csv(
        "outputs/kg/initialization/_pubchem_cids_fdc_manual.tsv", sep='\t',
    )

    def _parse_pubchem_cids(cids):
        res = None

        if pd.isna(cids):
            res = []
        elif cids.startswith('['):
            res = literal_eval(cids)
        else:
            res = [int(cids)]

        return res

    chemicals['pubchem_cid'] = chemicals['pubchem_cid'].apply(_parse_pubchem_cids)
    chemicals['name'] = chemicals['name'].str.strip().str.lower()
    chemicals = chemicals.set_index('id')

    def _parse_chemical_name(row):
        if len(row['pubchem_cid']) == 1:
            _chemical_name = constants.get_lookup_key_by_id(
                'pubchem_cid', row['pubchem_cid'][0]
            )

            # Just checking.
            assert '_placeholder_to' not in kg.entities._entities.loc[
                kg.entities._lut_chemical[_chemical_name][0]
            ]['external_ids']
        elif len(row['pubchem_cid']) > 1:
            _chemical_name = (
                row['name_formatted']
                if not row['name_formatted'].startswith('SKIP')
                else row['name']
            ).strip().lower()

            # Just checking.
            assert '_placeholder_to' in kg.entities._entities.loc[
                kg.entities._lut_chemical[_chemical_name][0]
            ]['external_ids']
        else:
            _chemical_name = row['name']

        return _chemical_name

    chemicals['_chemical_name'] = chemicals.apply(_parse_chemical_name, axis=1)

    return chemicals


def load_fdc_data():
    # Load relevant files.
    contains = _load_contains()
    foods = _load_foods()
    chemicals = _load_chemicals()

    return contains, foods, chemicals


if __name__ == '__main__':
    kg = KnowledgeGraph()
    entities = kg.entities._entities

    contains, foods, chemicals = load_fdc_data()
    nutrients = pd.read_csv(
        "data/FDC/FoodData_Central_foundation_food_csv_2023-10-26/nutrient.csv",
        usecols=['id', 'name', 'unit_name'],
    ).set_index('id')

    # Update entities.
    def _update_entities_food(row):
        global entities_new_rows

        synonyms_new = [
            row['description'], constants.get_lookup_key_by_id('fdc_ids', row.name)
        ]
        if row['_food_name'] in kg.entities._lut_food:
            entity_id = kg.entities._lut_food[row['_food_name']][0]

            # Add external IDs.
            if 'fdc_ids' not in entities.at[entity_id, 'external_ids']:
                entities.at[entity_id, 'external_ids']['fdc_ids'] = []
            entities.at[entity_id, 'external_ids']['fdc_ids'] += [row.name]

            # Add synonyms.
            entities.at[entity_id, 'synonyms'] += synonyms_new
            entities.at[entity_id, 'synonyms'] \
                = list(OrderedDict.fromkeys(entities.at[entity_id, 'synonyms']).keys())

            # Update the lookup table.
            for synonym in synonyms_new:
                if synonym in kg.entities._lut_food:
                    raise ValueError(f"Duplicate synonym: {synonym}")
                kg.entities._lut_food[synonym] = [entity_id]
        else:
            entities_new_rows += [{
                'foodatlas_id': f"e{kg.entities._curr_eid}",
                'entity_type': 'food',
                'common_name': row['description'],
                'scientific_name': '',
                'synonyms': synonyms_new,
                'external_ids': {'fdc_ids': [row.name]},
            }]
            for synonym in synonyms_new:
                if synonym in kg.entities._lut_food:
                    raise ValueError(f"Duplicate synonym: {synonym}")
                kg.entities._lut_food[synonym] = [f"e{kg.entities._curr_eid}"]
            kg.entities._curr_eid += 1

    def _update_entities_chemical(row):
        global entities_new_rows

        if row['_chemical_name'] in kg.entities._lut_chemical:
            entity_id = kg.entities._lut_chemical[row['_chemical_name']][0]

            # Add external IDs.
            if 'fdc_nutrient_ids' not in entities.at[entity_id, 'external_ids']:
                entities.at[entity_id, 'external_ids']['fdc_nutrient_ids'] = []
            entities.at[entity_id, 'external_ids']['fdc_nutrient_ids'] += [row.name]

            # Add synonyms.
            synonym_new = constants.get_lookup_key_by_id('fdc_nutrient_ids', row.name)
            entities.at[entity_id, 'synonyms'] += [synonym_new]

            # Update the lookup table.
            if synonym_new in kg.entities._lut_chemical:
                raise ValueError(f"Duplicate synonym: {synonym_new}")
            kg.entities._lut_chemical[synonym_new] = [entity_id]
        else:
            synonyms_new = [
                row['_chemical_name'],
                constants.get_lookup_key_by_id('fdc_nutrient_ids', row.name)
            ]

            entities_new_rows += [{
                'foodatlas_id': f"e{kg.entities._curr_eid}",
                'entity_type': 'chemical',
                'common_name': row['_chemical_name'],
                'scientific_name': '',
                'synonyms': synonyms_new,
                'external_ids': {'fdc_nutrient_ids': [row.name]},
            }]
            for synonym in synonyms_new:
                if synonym in kg.entities._lut_chemical:
                    raise ValueError(f"Duplicate synonym: {synonym}")
                kg.entities._lut_chemical[synonym] = [f"e{kg.entities._curr_eid}"]
            kg.entities._curr_eid += 1

    entities_new_rows = []
    foods.apply(_update_entities_food, axis=1)
    chemicals.apply(_update_entities_chemical, axis=1)
    entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
    kg.entities._entities = pd.concat([entities, entities_new])

    def _get_metadatum(row):
        global metadata_rows

        fdc_id = row['fdc_id']
        fdc_nutrient_id = row['nutrient_id']

        _food_name = foods.loc[fdc_id, '_food_name']
        _chemical_name = chemicals.loc[fdc_nutrient_id, '_chemical_name']

        if _food_name not in kg.entities._lut_food:
            print(f"Food not found: {_food_name}")
            return
        if _chemical_name not in kg.entities._lut_chemical:
            print(f"Chemical not found: {_chemical_name}")
            return

        conc_value = row['amount']
        conc_unit = f"{nutrients.loc[fdc_nutrient_id, 'unit_name'].lower()}/100g"

        metadata_row = {
            'conc_value': None,
            'conc_unit': None,
            'food_part': None,
            'food_processing': None,
            'source': 'fdc',
            'reference': {
                'url': "https://fdc.nal.usda.gov/fdc-app.html#/food-details/"
                f"{fdc_id}/nutrients",
            },
            'quality_score': None,
            '_food_name': _food_name,
            '_chemical_name': _chemical_name,
            '_conc': f'{conc_value} {conc_unit}',
            '_food_part': '',
        }

        metadata_rows += [metadata_row]

    metadata_rows = []
    contains.progress_apply(_get_metadatum, axis=1)
    metadata = pd.DataFrame(metadata_rows)
    metadata = standardize_chemical_conc(metadata)

    kg.add_triplets_from_metadata(metadata)
    test_all(kg)
    kg.save("outputs/kg/20240403_merged_fdc")

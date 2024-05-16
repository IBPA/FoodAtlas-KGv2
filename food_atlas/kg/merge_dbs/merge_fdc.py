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


def _load_contains(
    path_data_dir: str,
) -> pd.DataFrame:
    """
    """
    # Load relevant files.
    food_nutrient = pd.read_csv(
        f"{path_data_dir}/food_nutrient.csv",
        usecols=['id', 'fdc_id', 'nutrient_id', 'amount'],
    )
    fdc_ids_ff = pd.read_csv(f"{path_data_dir}/foundation_food.csv")['fdc_id']

    # Get chemical concentrations.
    food_nutrient = food_nutrient[food_nutrient['fdc_id'].isin(fdc_ids_ff)]
    food_nutrient = food_nutrient.query("nutrient_id != 2066").copy()

    return food_nutrient


def _load_foods(
    path_data_dir: str,
) -> pd.DataFrame:
    """
    """
    fdc_ids_ff = pd.read_csv(
        f"{path_data_dir}/foundation_food.csv"
    )['fdc_id']
    foods = pd.read_csv(
        f"{path_data_dir}/food.csv",
        usecols=['fdc_id', 'description'],
    ).set_index('fdc_id')
    foods = foods[foods.index.isin(fdc_ids_ff)]
    foods['description'] = foods['description'].str.strip().str.lower()

    # Get FoodOn IDs.
    food_attr = pd.read_csv(
        f"{path_data_dir}/food_attribute.csv",
    )
    food_attr = food_attr[food_attr['fdc_id'].isin(fdc_ids_ff)]
    food_attr = food_attr.set_index('fdc_id')

    def extract_urls(row):
        attr_name = 'FoodOn Ontology ID for FDC Item'

        # Targeted fixes: Deal with FDC items without FoodOn IDs.
        if row.name not in food_attr.index:
            if row.name == 2512381:
                foodon_url = 'http://purl.obolibrary.org/obo/FOODON_03000273'
            else:
                raise ValueError(f"FDC item without FoodOn ID exists: {row.name}")

            return pd.Series({'foodon_url': foodon_url})

        attr = food_attr.loc[row.name]
        if type(attr) == pd.Series:
            attr = pd.DataFrame(attr).T
        attr = attr.set_index('name')

        foodon_urls = attr.query(
            f"name == '{attr_name}'"
        )['value'].unique().tolist()

        if len(foodon_urls) == 0:
            # This case should not happen. Raise just in case.
            raise ValueError("FDC item without FoodOn ID exists.")
        elif len(foodon_urls) > 1:
            # Targeted fixes: Deal with multiple FoodOn URLs.
            if row.name == 323121:
                foodon_url = 'http://purl.obolibrary.org/obo/FOODON_03310577'
            elif row.name == 330137:
                foodon_url = 'http://purl.obolibrary.org/obo/FOODON_00004409'
            else:
                raise ValueError(f"Unaddressed multiple FoodOn URLs: {foodon_urls}")
        else:
            foodon_url = foodon_urls[0]

        return pd.Series({'foodon_url': foodon_url})

    foods = pd.concat(
        [foods, foods.apply(extract_urls, axis=1)],
        axis=1,
    )

    return foods[['description', 'foodon_url']]


def _load_chemicals(
    path_data_dir: str,
) -> pd.DataFrame:
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


def load_fdc_data(PATH_FDC_DATA_DIR):
    # Load relevant files.
    contains = _load_contains(PATH_FDC_DATA_DIR)
    foods = _load_foods(PATH_FDC_DATA_DIR)
    chemicals = _load_chemicals(PATH_FDC_DATA_DIR)

    return contains, foods, chemicals


if __name__ == '__main__':
    PATH_FDC_DATA_DIR = "data/FDC/FoodData_Central_foundation_food_csv_2024-04-18"

    kg = KnowledgeGraph()
    entities = kg.entities._entities
    lut_food = kg.entities._lut_food

    contains, foods, chemicals = load_fdc_data(PATH_FDC_DATA_DIR)

    nutrients = pd.read_csv(
        f"{PATH_FDC_DATA_DIR}/nutrient.csv",
        usecols=['id', 'name', 'unit_name'],
    ).set_index('id')

    # Create new entities for new foods in FDC.
    foods_new = foods[~foods['foodon_url'].apply(
        lambda x: constants.get_lookup_key_by_id('foodon_id', x) in lut_food
    )]
    entities_new_rows = []
    for group in foods_new.groupby('foodon_url'):
        foodon_id = group[0]
        group = group[1]

        synonyms = group['description'].tolist()
        synonyms += [constants.get_lookup_key_by_id('foodon_id', foodon_id)]
        synonyms += [
            constants.get_lookup_key_by_id('fdc_ids', x) for x in group.index.tolist()
        ]
        synonyms = list(OrderedDict.fromkeys(synonyms).keys())
        entities_new_rows += [{
            'foodatlas_id': f"e{kg.entities._curr_eid}",
            'entity_type': 'food',
            'common_name': group['description'].iloc[0],
            'scientific_name': '',
            'synonyms': synonyms,
            'external_ids': {'foodon_id': foodon_id, 'fdc_ids': group.index.tolist()},
        }]
        for synonym in synonyms:
            if synonym not in lut_food:
                lut_food[synonym] = []
            lut_food[synonym] += [f"e{kg.entities._curr_eid}"]
        kg.entities._curr_eid += 1
    entities_new_rows = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
    entities = pd.concat([entities, entities_new_rows])

    # # Update entities.
    # def _update_entities_food(row):
    #     synonyms_new = [
    #         row['description'],
    #         constants.get_lookup_key_by_id('fdc_ids', row.name),
    #     ]

    #     foodon_id = row['foodon_url']

    #     # Use FoodOn IDs to identify entities.
    #     if constants.get_lookup_key_by_id('foodon_id', foodon_id) in lut_food:
    #         entity_id = lut_food[
    #             constants.get_lookup_key_by_id('foodon_id', foodon_id)
    #         ][0]

    #         # Add external IDs.
    #         if 'fdc_ids' not in entities.at[entity_id, 'external_ids']:
    #             entities.at[entity_id, 'external_ids']['fdc_ids'] = []
    #         entities.at[entity_id, 'external_ids']['fdc_ids'] += [row.name]

    #         # Add synonyms.
    #         entities.at[entity_id, 'synonyms'] += synonyms_new
    #         entities.at[entity_id, 'synonyms'] \
    #             = list(OrderedDict.fromkeys(entities.at[entity_id, 'synonyms']).keys())

    #         # Update the lookup table.
    #         for synonym in synonyms_new:
    #             if synonym in lut_food:
    #                 raise ValueError(f"Duplicate synonym: {synonym}")
    #             lut_food[synonym] = [entity_id]
    #     else:
    #         raise ValueError("This should not happen.")
    #         synonyms_new += [constants.get_lookup_key_by_id('foodon_id', foodon_id)]
    #         entity_new = pd.DataFrame([{
    #             'foodatlas_id': f"e{kg.entities._curr_eid}",
    #             'entity_type': 'food',
    #             'common_name': row['description'],
    #             'scientific_name': '',
    #             'synonyms': synonyms_new,
    #             'external_ids': {'foodon_id': foodon_id, 'fdc_ids': [row.name]},
    #         }])
    #         kg.entities._entities = pd.concat([
    #             entities,
    #             entity_new.set_index('foodatlas_id'),
    #         ])
    #         print(kg.entities._entities)
    #         # if foodon_id == 'http://purl.obolibrary.org/obo/FOODON_00003364':
    #         #     print("Here")
    #         #     print(f"e{kg.entities._curr_eid}")
    #         for synonym in synonyms_new:
    #             if synonym in lut_food:
    #                 raise ValueError(f"Duplicate synonym: {synonym}")
    #             lut_food[synonym] = [f"e{kg.entities._curr_eid}"]
    #         kg.entities._curr_eid += 1

    def _update_entities_chemical(row):
        global entities_new_rows, entities

        if row['_chemical_name'] in kg.entities._lut_chemical:
            entity_id = kg.entities._lut_chemical[row['_chemical_name']][0]

            if entity_id not in entities.index:
                entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
                kg.entities._entities = pd.concat([entities, entities_new])
                entities = kg.entities._entities
                entities_new_rows = []

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
    # foods.apply(_update_entities_food, axis=1)
    chemicals.apply(_update_entities_chemical, axis=1)
    entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
    kg.entities._entities = pd.concat([entities, entities_new])

    def _get_metadatum(row):
        global metadata_rows

        fdc_id = row['fdc_id']
        fdc_nutrient_id = int(row['nutrient_id'])

        _food_name = constants.get_lookup_key_by_id(
            'foodon_id', foods.loc[fdc_id, 'foodon_url']
        )
        _chemical_name = constants.get_lookup_key_by_id(
            'fdc_nutrient_ids', fdc_nutrient_id
        )

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
                f"{int(fdc_id)}/nutrients",
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
    kg.save("outputs/kg")

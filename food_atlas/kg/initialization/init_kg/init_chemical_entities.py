from ast import literal_eval
from collections import defaultdict
import re

import pandas as pd

from ...utils import load_entities


def create_entities_from_pubchem_compound():
    entities = load_entities("outputs/kg/initialization/entities.tsv")
    data = pd.read_csv(
        "outputs/kg/initialization/_pubchem_compound.tsv",
        sep='\t',
        converters={'SynonymList': literal_eval},
    )
    data['iupac_name'] = data['IUPACName'].str.strip().str.lower()
    data['synonyms'] \
        = data['SynonymList'].apply(lambda x: [s.strip().lower() for s in x])

    # Create new entities for the rest of the foods.
    def _merge_entity(row):
        nonlocal foodatlas_id_curr
        nonlocal entities_new_rows

        # Pick a common name.
        synonyms_all_digits = [
            True if re.match('[\d-]+$', s) else False for s in row['synonyms']
        ]
        if all(synonyms_all_digits):
            common_name = row['iupac_name']
        else:
            common_name = row['synonyms'][synonyms_all_digits.index(False)]

        entities_new_rows += [{
            'foodatlas_id': f"e{foodatlas_id_curr}",
            'entity_type': 'chemical',
            'common_name': common_name,
            'scientific_name': row['iupac_name'],
            'synonyms': list(set(row['synonyms'] + [row['iupac_name']])),
            'external_ids': {'pubchem_cid': row['CID']},
        }]
        foodatlas_id_curr += 1

    foodatlas_id_curr = entities['foodatlas_id'].str.slice(1).astype(int).max() + 1
    entities_new_rows = []
    data.apply(lambda row: _merge_entity(row), axis=1)
    entities = pd.concat([entities, pd.DataFrame(entities_new_rows)])

    # Initialize the lookup table.
    lut = defaultdict(list)
    entitie_chemicals = entities.query("entity_type == 'chemical'")

    def _add_to_lut(row, lut):
        for name in row['synonyms']:
            lut[name] += [row['foodatlas_id']]

    entitie_chemicals.apply(lambda row: _add_to_lut(row, lut), axis=1)

    # count_ambiguous = 0
    # pubchem_cids_ambiguous = set()
    # for k, v in lut.items():
    #     if len(v) > 1:
    #         count_ambiguous += 1
    #         pubchem_cids_ambiguous.update(v)
    #         print(k)
    #         print(v)
    #         print()

    # print(f"Number of unique PubChem CIDs: {len(entities)}")
    # print(f"Number of ambiguous PubChem CIDs: {len(pubchem_cids_ambiguous)}")
    # print(f"Number of ambiguous names: {count_ambiguous} out of {len(lut)}")

    return entities, lut


def append_entities_from_fdc(entities, lut):
    data = pd.read_csv(
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

    data['pubchem_cid'] = data['pubchem_cid'].apply(_parse_pubchem_cids)
    data['name'] = data['name'].str.strip().str.lower()
    entities_chemical = entities.query("entity_type == 'chemical'").copy()
    entities_chemical['pubchem_cid'] = entities_chemical['external_ids'].apply(
        lambda x: x['pubchem_cid']
    )
    entities_chemical = entities_chemical.set_index('pubchem_cid')
    entities = entities.set_index('foodatlas_id')

    def _merge_entities_by_pubchem_cid(row):
        nonlocal entities
        nonlocal lut

        for cid in row['pubchem_cid']:
            foodatlas_id = entities_chemical.loc[cid, 'foodatlas_id']
            entity = entities.loc[foodatlas_id]

            # Expand the external IDs.
            if 'fdc_nutrient_ids' not in entity['external_ids']:
                entity['external_ids']['fdc_nutrient_ids'] = []
            entity['external_ids']['fdc_nutrient_ids'] += [row['id']]

            # Expand the synonyms
            if row['name'] in entity['synonyms']:
                # This means the name is already in the lookup table.
                continue
            else:
                # Either the synonym in the lookup table but not linked to the entity,
                # or the synonym is not in the lookup table. In both cases, we need to
                # add the synonym to the entity and the lookup table.
                entity['synonyms'] += [row['name']]
                lut[row['name']] += [foodatlas_id]

    data[data['pubchem_cid'].apply(lambda x: True if len(x) else False)].apply(
        _merge_entities_by_pubchem_cid, axis=1
    )

    def _expand_entities(row):
        nonlocal entities
        nonlocal lut
        nonlocal foodatlas_id_curr
        nonlocal entities_new_rows

        entities_new_rows += [{
            'foodatlas_id': f"e{foodatlas_id_curr}",
            'entity_type': 'chemical',
            'common_name': row['name'],
            'scientific_name': '',
            'synonyms': [row['name']],
            'external_ids': {'fdc_nutrient_ids': [row['id']]},
        }]
        lut[row['name']] += [f"e{foodatlas_id_curr}"]
        foodatlas_id_curr += 1

    entities = entities.reset_index()
    foodatlas_id_curr = entities['foodatlas_id'].str.slice(1).astype(int).max() + 1
    entities_new_rows = []
    data[
        ~data['pubchem_cid'].apply(lambda x: True if len(x) else False)
    ].apply(
        _expand_entities, axis=1
    )
    entities = pd.concat([entities, pd.DataFrame(entities_new_rows)], ignore_index=True)

    return entities, lut


if __name__ == '__main__':
    entities, lut = create_entities_from_pubchem_compound()
    entities, lut = append_entities_from_fdc(entities, lut)

    # Save the results.
    entities.to_csv("outputs/kg/initialization/entities.tsv", sep='\t', index=False)
    pd.DataFrame(lut.items(), columns=['name', 'foodatlas_id']).to_csv(
        "outputs/kg/initialization/lookup_table_chemical.tsv", sep='\t', index=False
    )

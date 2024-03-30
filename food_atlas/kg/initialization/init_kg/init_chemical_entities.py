from ast import literal_eval
from collections import defaultdict
import re

import pandas as pd

from ... import KnowledgeGraph
from ....tests import unit_test_kg


def initialize_chemical_entities_from_pubchem_compound(kg):
    records = pd.read_csv(
        "outputs/kg/initialization/_pubchem_compound.tsv",
        sep='\t',
        converters={'SynonymList': literal_eval},
    )
    records['iupac_name'] = records['IUPACName'].str.strip().str.lower()
    records['synonyms'] = records['SynonymList'].apply(
        lambda x: [s.strip().lower() for s in x]
    )

    kg.entities._create_chemical_entities_from_pubchem_compound(records)

    # # Create new entities for the rest of the foods.
    # def _merge_entity(row):
    #     nonlocal foodatlas_id_curr
    #     nonlocal entities_new_rows

    #     # Pick a common name.
    #     synonyms_all_digits = [
    #         True if re.match('[\d-]+$', s) else False for s in row['synonyms']
    #     ]
    #     if all(synonyms_all_digits):
    #         common_name = row['iupac_name']
    #     else:
    #         common_name = row['synonyms'][synonyms_all_digits.index(False)]

    #     entities_new_rows += [{
    #         'foodatlas_id': f"e{foodatlas_id_curr}",
    #         'entity_type': 'chemical',
    #         'common_name': common_name,
    #         'scientific_name': row['iupac_name'],
    #         'synonyms': list(set(row['synonyms'] + [row['iupac_name']])),
    #         'external_ids': {'pubchem_cid': row['CID']},
    #     }]
    #     foodatlas_id_curr += 1

    # foodatlas_id_curr = entities['foodatlas_id'].str.slice(1).astype(int).max() + 1
    # entities_new_rows = []
    # data.apply(lambda row: _merge_entity(row), axis=1)
    # entities = pd.concat([entities, pd.DataFrame(entities_new_rows)])

    # # Initialize the lookup table.
    # lut = defaultdict(list)
    # entitie_chemicals = entities.query("entity_type == 'chemical'")

    # def _add_to_lut(row, lut):
    #     for name in row['synonyms']:
    #         lut[name] += [row['foodatlas_id']]

    # entitie_chemicals.apply(lambda row: _add_to_lut(row, lut), axis=1)

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

    # return entities, lut


if __name__ == '__main__':
    kg = KnowledgeGraph(
        path_kg="outputs/kg/initialization",
    )
    initialize_chemical_entities_from_pubchem_compound(kg)
    kg._disambiguate_synonyms()
    kg.save("outputs/kg/initialization")
    unit_test_kg.test_all(kg)

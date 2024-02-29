import pandas as pd

COLUMNS = [
    'foodatlas_id',
    'entity_type',
    'common_name',
    'scientific_name',
    'synonyms',
    'external_ids',
]


# def _create_entities_food(
#     self,
# ):
#     if not os.path.exists(f"{self.path_output_dir}/_id_ncbi_taxonomy.tsv"):
#         raise FileNotFoundError(
#             f"The file {self.path_output_dir}/_id_ncbi_taxonomy.tsv is required. "
#             "Please run the following command: "
#             "- `python -m food_atlas.kg.retrieve_ncbi_taxonomy`"
#         )
#     n_entities_old = len(self._entities)
#     n_lut_entries_old = len(self._lut_food)

#     data = pd.read_csv(
#         f"{self.path_output_dir}/_id_ncbi_taxonomy.tsv",
#         sep='\t',
#         converters={
#             'OtherNames': lambda x: literal_eval(x) if x != '' else None,
#         },
#     )

#     def _get_entity(row):
#         # Get synonyms.
#         row['scientific_name'] = row['ScientificName'].strip().lower()

#         synonyms = []
#         other_names = row['OtherNames']
#         if other_names is not None:
#             synonyms += other_names['Synonym']
#             synonyms += other_names['EquivalentName']
#             synonyms += other_names['CommonName']
#             synonyms += other_names['Includes']
#             if 'GenbankCommonName' in other_names:
#                 synonyms += [other_names['GenbankCommonName']]
#             if 'BlastName' in other_names:
#                 synonyms += [other_names['BlastName']]
#             if other_names['Name']:
#                 for name in other_names['Name']:
#                     if name['ClassCDE'] == 'misspelling':
#                         synonyms += [name['DispName']]
#         synonyms += [row['scientific_name']]
#         row['synonyms'] = [synonym.strip().lower() for synonym in synonyms]
#         row['common_name'] = min(row['synonyms'], key=len)

#         row['foodatlas_id'] = f"e{self._curr_eid}"
#         row['entity_type'] = 'food'
#         row['external_ids'] = {'ncbi_taxon_id': row['TaxId']}
#         self._curr_eid += 1

#         return row

#     columns = [
#         'foodatlas_id', 'entity_type', 'common_name', 'scientific_name', 'synonyms',
#         'external_ids',
#     ]
#     data[columns] = None
#     data = data.apply(lambda row: _get_entity(row), axis=1)

#     entities = data[[
#         'foodatlas_id',
#         'entity_type',
#         'common_name',
#         'scientific_name',
#         'synonyms',
#         'external_ids',
#     ]]
#     self._entities = pd.concat([self._entities, entities], ignore_index=True)
#     names_not_in_lut_food = self._update_lookup_table_food(entities)

#     # Remove duplicates.
#     # TODO: Optimize: maybe as part of the process when updating the lookup table.
#     names_not_in_lut_food = [sorted(names) for names in names_not_in_lut_food]
#     names_not_in_lut_food = ['@SEP@'.join(names) for names in names_not_in_lut_food]
#     visited = {}
#     entities_new_rows = []
#     for names_bundle_str in names_not_in_lut_food:
#         if names_bundle_str in visited:
#             continue
#         names_bundle = names_bundle_str.split('@SEP@')
#         entities_new_rows += [{
#             'foodatlas_id': f"e{self._curr_eid}",
#             'entity_type': 'food',
#             'common_name': min(names_bundle, key=len),
#             'scientific_name': None,
#             'synonyms': names_bundle,
#             'external_ids': {},
#         }]
#         visited[names_bundle_str] = True
#         self._curr_eid += 1

#     entities_new = pd.DataFrame(entities_new_rows)

#     def _update_lut(row):
#         for synonym in row['synonyms']:
#             if synonym not in self._lut_food:
#                 self._lut_food[synonym] = []
#             self._lut_food[synonym] += [row['foodatlas_id']]

#     entities_new.apply(_update_lut, axis=1)
#     self._entities = pd.concat([self._entities, entities_new], ignore_index=True)

#     self._entities.to_csv(
#         f"{self.path_output_dir}/entities.tsv",
#         sep='\t',
#         index=False,
#     )

#     pd.DataFrame(self._lut_food.items(), columns=['name', 'foodatlas_id']).to_csv(
#         f"{self.path_output_dir}/lookup_table_food.tsv", sep='\t', index=False
#     )

#     print(f"# of new food entities added: {len(self._entities) - n_entities_old}")
#     print(
#         "# of new food lookup table entries added: "
#         f"{len(self._lut_food) - n_lut_entries_old}"
#     )

# def _create_entities_chemical(
#     self,
# ):
#     """
#     """
#     if not os.path.exists(f"{self.path_output_dir}/_id_pubchem_compound.tsv"):
#         raise FileNotFoundError(
#             f"The file {self.path_output_dir}/_id_pubchem_compound.tsv is "
#             "required. Please run the following command: "
#             "- `python -m food_atlas.kg.retrieve_pubchem_compound`"
#         )

#     n_entities_old = len(self._entities)
#     n_lut_entries_old = len(self._lut_chemical)

#     data = pd.read_csv(
#         f"{self.path_output_dir}/_id_pubchem_compound.tsv",
#         sep='\t',
#         converters={'SynonymList': literal_eval},
#     )
#     data['iupac_name'] = data['IUPACName'].str.strip().str.lower()
#     data['synonyms'] = data.apply(
#         lambda row: [s.strip().lower() for s in row['SynonymList']],
#         axis=1,
#     )

#     def _get_entity(row):
#         row['scientific_name'] = row['iupac_name']
#         row['synonyms'] += [row['iupac_name']] \
#             if not pd.isna(row['iupac_name']) else []
#         row['synonyms'] = list(set(row['synonyms']))
#         row['foodatlas_id'] = f"e{self._curr_eid}"
#         row['entity_type'] = 'chemical'
#         row['external_ids'] = {'pubchem_cid': row['CID']}
#         self._curr_eid += 1

#         # Pick a common name.
#         synonyms_all_digits = [
#             True if re.match('[\d-]+$', s) else False for s in row['synonyms']
#         ]
#         if all(synonyms_all_digits):
#             row['common_name'] = row['iupac_name']
#         else:
#             row['common_name'] = row['synonyms'][synonyms_all_digits.index(False)]

#         return row

#     columns = [
#         'foodatlas_id', 'entity_type', 'common_name', 'scientific_name', 'synonyms',
#         'external_ids',
#     ]
#     data[[
#         'foodatlas_id',
#         'entity_type',
#         'common_name',
#         'scientific_name',
#         'external_ids',
#     ]] = None
#     data = data.apply(lambda row: _get_entity(row), axis=1)
#     entities = data[columns]
#     self._entities = pd.concat([self._entities, entities], ignore_index=True)

#     # Add entities without CIDs.
#     names_not_in_lut = self._update_lookup_table_chemical(entities)
#     entities_new_rows = []
#     for name in names_not_in_lut:
#         entities_new_rows += [{
#             'foodatlas_id': f"e{self._curr_eid}",
#             'entity_type': 'chemical',
#             'common_name': name,
#             'scientific_name': None,
#             'synonyms': [name],
#             'external_ids': {},
#         }]
#         self._curr_eid += 1
#     entities_new = pd.DataFrame(entities_new_rows)

#     def _update_lut(row):
#         for synonym in row['synonyms']:
#             if synonym not in self._lut_chemical:
#                 self._lut_chemical[synonym] = []
#             self._lut_chemical[synonym] += [row['foodatlas_id']]

#     entities_new.apply(_update_lut, axis=1)
#     self._entities = pd.concat([self._entities, entities_new], ignore_index=True)

#     self._entities.to_csv(
#         f"{self.path_output_dir}/entities.tsv",
#         sep='\t',
#         index=False,
#     )

#     pd.DataFrame(
#         self._lut_chemical.items(),
#         columns=['name', 'foodatlas_id'],
#     ).to_csv(
#         f"{self.path_output_dir}/lookup_table_chemical.tsv", sep='\t', index=False
#     )

#     print(
#         f"# of new chemical entities added: {len(self._entities) - n_entities_old}"
#     )
#     print(
#         "# of new chemical lookup table entries added: "
#         f"{len(self._lut_chemical) - n_lut_entries_old}"
#     )


def _append_entity_food(
    entities_new_identified: pd.DataFrame,
    entities_new_unidentified: list,
    entities: pd.DataFrame,
    lut: dict,
    curr_eid: int,
):
    """Create new entities for food. An entity is defined by the fields:
        - 'foodatlas_id'
        - 'entity_type'
        - 'common_name'
        - 'scientific_name'
        - 'synonyms'
        - 'external_ids'

    Args:
        entities_identified: New entities found and retrieved from NCBI Taxonomy.
        entities_unidentified: New entities that were not found in NCBI Taxonomy.

    """
    n_entities_old = len(entities)
    n_lut_entries_old = len(lut)

    def _get_entity(row):
        nonlocal curr_eid

        row['scientific_name'] = row['ScientificName'].strip().lower()

        # Get synonyms.
        synonyms = []
        other_names = row['OtherNames']
        if other_names is not None:
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
        synonyms += [row['scientific_name']]
        row['synonyms'] = list(set([synonym.strip().lower() for synonym in synonyms]))
        row['common_name'] = min(row['synonyms'], key=len)

        row['foodatlas_id'] = f"e{curr_eid}"
        row['entity_type'] = 'food'
        row['external_ids'] = {'ncbi_taxon_id': row['TaxId']}
        curr_eid += 1

        return row

    entities_new_identified[COLUMNS] = None
    entities_new_identified \
        = entities_new_identified.apply(lambda row: _get_entity(row), axis=1)

    entities = pd.concat([entities, entities_new_identified], ignore_index=True)

    names_not_in_lut_food = self._update_lookup_table_food(entities)

    return entities

#     # Remove duplicates.
#     # TODO: Optimize: maybe as part of the process when updating the lookup table.
#     names_not_in_lut_food = [sorted(names) for names in names_not_in_lut_food]
#     names_not_in_lut_food = ['@SEP@'.join(names) for names in names_not_in_lut_food]
#     visited = {}
#     entities_new_rows = []
#     for names_bundle_str in names_not_in_lut_food:
#         if names_bundle_str in visited:
#             continue
#         names_bundle = names_bundle_str.split('@SEP@')
#         entities_new_rows += [{
#             'foodatlas_id': f"e{self._curr_eid}",
#             'entity_type': 'food',
#             'common_name': min(names_bundle, key=len),
#             'scientific_name': None,
#             'synonyms': names_bundle,
#             'external_ids': {},
#         }]
#         visited[names_bundle_str] = True
#         self._curr_eid += 1

#     entities_new = pd.DataFrame(entities_new_rows)

#     def _update_lut(row):
#         for synonym in row['synonyms']:
#             if synonym not in self._lut_food:
#                 self._lut_food[synonym] = []
#             self._lut_food[synonym] += [row['foodatlas_id']]

#     entities_new.apply(_update_lut, axis=1)
#     self._entities = pd.concat([self._entities, entities_new], ignore_index=True)

#     self._entities.to_csv(
#         f"{self.path_output_dir}/entities.tsv",
#         sep='\t',
#         index=False,
#     )

#     pd.DataFrame(self._lut_food.items(), columns=['name', 'foodatlas_id']).to_csv(
#         f"{self.path_output_dir}/lookup_table_food.tsv", sep='\t', index=False
#     )

#     print(f"# of new food entities added: {len(self._entities) - n_entities_old}")
#     print(
#         "# of new food lookup table entries added: "
#         f"{len(self._lut_food) - n_lut_entries_old}"
#     )


def create_entity_chemical():
    pass


class Entities:
    """
    """

    def __init__(self, path_data: str):
        pass

    def create(self):
        pass

    def update(self):
        pass

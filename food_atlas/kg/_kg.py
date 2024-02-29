import os
import re
from ast import literal_eval
from itertools import product

import pandas as pd
from inflection import singularize, pluralize
from pympler import asizeof

from .utils import load_entities, load_lookup_tables, load_mdata, constants

from ._lut import LookupTables
from ._metadata import Metadata
from ._triplets import Triplets


class KnowledgeGraph:
    """
    """

    def __init__(
        self,
        path_kg="outputs/kg",
        path_output_dir=None,
    ):
        self.path_kg = path_kg
        self.path_output_dir = path_output_dir
        self._load()
        # self.print_stats()

    def _load(self):
        """Load the knowledge graph data from the `self.path_kg`.

        """
        # Load the knowledge graph data.
        self._entities = load_entities(f"{self.path_kg}/entities.tsv")
        self._relations = pd.read_csv(f"{self.path_kg}/relationships.tsv", sep='\t')
        # self._triplets = pd.read_csv(
        #     f"{self.path_kg}/triplets.tsv",
        #     sep='\t',
        #     converters={'metadata_ids': literal_eval},
        # )

        self.luts = LookupTables(
            path_lut_food=f"{self.path_kg}/lookup_table_food.tsv",
            path_lut_chemical=f"{self.path_kg}/lookup_table_chemical.tsv",
        )
        self.metadata = Metadata(
            path_metadata_contains=f"{self.path_kg}/metadata_contains.tsv",
        )
        self.triplets = Triplets(
            path_triplets=f"{self.path_kg}/triplets.tsv",
        )

        # Keep track of the current maximum entity IDs.
        self._curr_eid \
            = self._entities['foodatlas_id'].str.slice(1).astype(int).max() + 1

    def _save(self):
        """
        """
        self.luts._save(self.path_output_dir)
        self.metadata._save(self.path_output_dir)
        self.triplets._save(self.path_output_dir)

    # def _load_lut_triplets(self):
    #     self._lut_triplets = dict(zip(
    #         self._triplets.apply(
    #             lambda row:
    #                 f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}",
    #             axis=1,
    #         ),
    #         self._triplets['metadata_ids'],
    #     ))

    # def _load_lut_mdata(self):
    #     self._lut_mdata = {}

    #     def _update_lut(row):
    #         for tid in row['tids']:
    #             if tid not in self._lut_mdata:
    #                 self._lut_mdata[tid] = []
    #             self._lut_mdata[tid] += [row['foodatlas_id']]

    #     self._mdata_contains.apply(_update_lut, axis=1)

    # def print_stats(self):
    #     print(f"KG space consumption: {asizeof.asizeof(self) / 1024 / 1024:.2f} MB")
    #     print(f"# of entities: {len(self._entities)}")
    #     print(f"# of triplets: {len(self._triplets)}")

    # def get_triplets_by_food_name(self, food_name):
    #     """
    #     """
    #     eids = self._lut_food[food_name]
    #     tids = self._triplets.loc[
    #         self._triplets['head_id'].isin(eids), 'foodatlas_id'
    #     ].tolist()

    #     triplets = self._triplets.set_index('foodatlas_id')
    #     mcids = []
    #     for tid in tids:
    #         mcids += triplets.loc[tid, 'metadata_ids']
    #     mcids = list(set(mcids))

    #     mdata = self._mdata_contains.set_index('foodatlas_id').loc[mcids]

    #     return mdata

    def get_triplets(
        self,
        head_name: str,
        relationship_name: str,
        tail_name: str,
    ):
        """
        """
        if relationship_name == 'contains':
            head_type = 'food'
            tail_type = 'chemical'
            head_ids = self._link_name_to_entities(head_name, head_type)
            tail_ids = self._link_name_to_entities(tail_name, tail_type)
        else:
            raise NotImplementedError

        tids = []
        for head_id, tail_id in product(head_ids, tail_ids):
            tids += [self._lut_triplets[f"{head_id}_r1_{tail_id}"]]

        return tids

    # def _update_lookup_table_food(
    #     self,
    #     entities: pd.DataFrame,
    # ):
    #     """
    #     """
    #     # Step 1. Update the lookup table using the synonyms of the new entities.
    #     def _update_lut(row):
    #         ncbi_taxon_id = row['external_ids']['ncbi_taxon_id']
    #         name_id = constants.get_lookup_key_by_id('ncbi_taxon_id', ncbi_taxon_id)
    #         for name in row['synonyms'] + [name_id]:
    #             if name not in self._lut_food:
    #                 self._lut_food[name] = []
    #             self._lut_food[name] += [row['foodatlas_id']] if row['foodatlas_id'] \
    #                 not in self._lut_food[name] else []

    #     entities.apply(_update_lut, axis=1)

    #     # Step 2. Update the lookup table for the name bundles.
    #     names_bundled = pd.read_csv(
    #         f"{self.path_output_dir}/_names_bundled.txt",
    #         names=['names_bundled'],
    #         header=None,
    #         sep='\t',
    #         converters={0: lambda x: literal_eval(x)},
    #     )

    #     def _update_entities_bundle(names_bundled):
    #         nonlocal names_not_in_lut_food

    #         def _update_helper(row, names_bundled):
    #             for name in names_bundled:
    #                 if name not in row['synonyms']:
    #                     eid = row.name
    #                     if name not in self._lut_food:
    #                         self._lut_food[name] = []
    #                     self._lut_food[name] \
    #                         += [eid] if eid not in self._lut_food[name] else []
    #                     row['synonyms'] += [name]

    #         found = False
    #         for name in names_bundled:
    #             if name in self._lut_food:
    #                 # Found a match, add the entirer bundle to the entity synonyms.
    #                 found = True
    #                 eids_matched = self._lut_food[name]
    #                 entities_updated = self._entities.loc[eids_matched]
    #                 entities_updated.apply(
    #                     lambda row: _update_helper(row, names_bundled), axis=1
    #                 )
    #         if not found:
    #             names_not_in_lut_food += [names_bundled]

    #     names_not_in_lut_food = []
    #     self._entities = self._entities.set_index('foodatlas_id')
    #     names_bundled['names_bundled'].apply(_update_entities_bundle)
    #     self._entities = self._entities.reset_index()

    #     return names_not_in_lut_food

    # def _update_lookup_table_chemical(
    #     self,
    #     entities: pd.DataFrame,
    # ):
    #     """
    #     """
    #     # Step 1. Update the lookup table using the synonyms of the new entities.
    #     def _update_lut(row):
    #         cid = row['external_ids']['pubchem_cid']
    #         name_id = constants.get_lookup_key_by_id('pubchem_cid', cid)
    #         for name in row['synonyms'] + [name_id]:
    #             if name not in self._lut_chemical:
    #                 self._lut_chemical[name] = []
    #             self._lut_chemical[name] \
    #                 += [row['foodatlas_id']] \
    #                     if row['foodatlas_id'] not in self._lut_chemical[name] else []

    #     entities.apply(_update_lut, axis=1)

    #     # Step 2. Get the names not in the lookup table.
    #     names = pd.read_csv(
    #         f"{self.path_output_dir}/_names_not_in_lut_chemical.txt",
    #         header=None,
    #         sep='\t',
    #     )[0]

    #     names_not_in_lut = []
    #     for name in names:
    #         if name not in self._lut_chemical:
    #             names_not_in_lut += [name]

    #     return names_not_in_lut

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


    # def _get_new_triplets_from_mdatum(
    #     self,
    #     head_ids: list[str],
    #     relationship_id: str,
    #     tail_id: list[str],
    # ) -> tuple[list[dict], dict]:
    #     """
    #     """
    #     triplets_new_rows = []
    #     mdata_new_row = {
    #         'foodatlas_id': None,
    #         # 'tids': [],
    #     }
    #     for head_id, tail_id in product(head_ids, tail_id):
    #         if f"{head_id}_{relationship_id}_{tail_id}" not in self._lut_triplets:
    #             self._lut_triplets[f"{head_id}_{relationship_id}_{tail_id}"] = []
    #             triplets_new_rows += [{
    #                 'foodatlas_id': f"t{self._curr_tid}",
    #                 'head_id': head_id,
    #                 'relationship_id': relationship_id,
    #                 'tail_id': tail_id,
    #                 'metadata_ids': None,
    #             }]
    #             self._curr_tid += 1

    #         self._lut_triplets[f"{head_id}_{relationship_id}_{tail_id}"] \
    #             += [f"mc{self._curr_mcid}"]
    #         # mdata_new_row['tids'] += [
    #         #     self._lut_triplets[f"{head_id}_{relationship_id}_{tail_id}"]
    #         # ]

    #     return triplets_new_rows, mdata_new_row

    # def _get_updated_triplets(
    #     self,
    #     head_ids: list[str],
    #     relationship_id: str,
    #     tail_id: list[str],
    #     row: pd.Series,
    # ) -> tuple[list[dict], dict]:
    #     """
    #     """
    #     triplets_new_rows, mdata_new_row \
    #         = self._get_new_triplets_from_mdatum(head_ids, relationship_id, tail_id)

    #     if relationship_id == 'r1':
    #         mdata_new_row.update({
    #             '_rid': 'r1',
    #             'foodatlas_id': f"mc{self._curr_mcid}",
    #             'food_name': row['head'],
    #             'chemical_name': row['tail'],
    #             'conc_value': row['conc_value'],  # TODO: Call parse_conc.
    #             'conc_unit': row['conc_unit'],  # TODO: Call parse_conc.
    #             'food_part': row['food_part'],  # TODO: Call parse_food_part.
    #             'food_processing': row['food_processing'],  # TODO: Call parse_food_processing.
    #             'source': row['source'],
    #             'reference': row['reference'],
    #             'quality_score': row['quality_score'],  # TODO: Call predict_quality.
    #             '_extracted_conc': row['_extracted_conc'],
    #             '_extracted_food_part': row['_extracted_food_part'],
    #         })
    #         self._curr_mcid += 1
    #     else:
    #         raise NotImplementedError

    #     return triplets_new_rows, mdata_new_row

    # def _link_name_to_entities(
    #     self,
    #     entity_name: str,
    #     entity_type: str,
    # ) -> list[str]:
    #     """Return the entity ID for the given entity name and type.

    #     Args:
    #         entity_name: The name of the entity.
    #         entity_type: The type of the entity.

    #     Returns:
    #         A list of foodatlas_ids for entities.

    #     """
    #     if entity_type == 'food':
    #         if entity_name in self._lut_food:
    #             return self._lut_food[entity_name]
    #     elif entity_type == 'chemical':
    #         if entity_name in self._lut_chemical:
    #             return self._lut_chemical[entity_name]
    #     else:
    #         raise NotImplementedError

    #     return []

    # def _dump_new_names_contains(
    #     self,
    #     triplets: pd.DataFrame,
    # ):
    #     """
    #     """
    #     # Step 0. Group the names such that they will be always added together when
    #     # updating the lookup table.
    #     # ht = {}
    #     names = triplets['head'].unique()
    #     names_bundled = [
    #         list(set([name, singularize(name), pluralize(name)])) for name in names
    #     ]
    #     names_bundled = pd.Series(names_bundled)
    #     names_bundled.to_csv(
    #         f"{self.path_output_dir}/_names_bundled.txt",
    #         index=False,
    #         header=False,
    #         sep='\t',
    #     )

    #     # Step 1. Get all food names and apply singularize and pluralize.
    #     # Store the ones not found in the lookup table.
    #     names_all_food = set()
    #     for name in triplets['head'].unique():
    #         names_all_food |= set([name, singularize(name), pluralize(name)])
    #     names_all_food = list(names_all_food)

    #     names_not_in_lut_food = []
    #     for name in names_all_food:
    #         if not self._link_name_to_entities(name, 'food'):
    #             names_not_in_lut_food += [name]

    #     pd.Series(names_not_in_lut_food).to_csv(
    #         f"{self.path_output_dir}/_names_not_in_lut_food.txt",
    #         sep='\t',
    #         index=False,
    #         header=False,
    #     )

    #     # Step 2. Get all chemical names. Store the ones not found in the lookup table.
    #     names_all_chemical = triplets['tail'].unique()

    #     names_not_in_lut_chemical = []
    #     for name in names_all_chemical:
    #         if not self._link_name_to_entities(name, 'chemical'):
    #             names_not_in_lut_chemical += [name]

    #     pd.Series(names_not_in_lut_chemical).to_csv(
    #         f"{self.path_output_dir}/_names_not_in_lut_chemical.txt",
    #         sep='\t',
    #         index=False,
    #         header=False,
    #     )

    #     print(f"# of unique name for food: {len(names_all_food)}")
    #     print(f"# of unique name for chemical: {len(names_all_chemical)}")

    #     if names_not_in_lut_food or names_not_in_lut_chemical:
    #         print(
    #             f"# of unique name for food not in LUT: {len(names_not_in_lut_food)}"
    #         )
    #         print(
    #             "# of unique name for chemical not in LUT: "
    #             f"{len(names_not_in_lut_chemical)}"
    #         )
    #         return True
    #     else:
    #         return False

    # def add_triplets(
    #     self,
    #     triplets: pd.DataFrame,
    # ):
    #     """Add triplets to the knowledge graph. Create new triplets and metadata files
    #     for successfully added triplets. Create triplets not found in the lookup tables.

    #     """
    #     # Sanity checks.
    #     for col in ['head', 'relationship', 'tail']:
    #         if col not in triplets.columns:
    #             raise ValueError(f"Column '{col}' not found in triplets")

    #     for relationship in triplets['relationship'].unique():
    #         if relationship == 'contains':
    #             is_dumped = self._dump_new_names_contains(triplets)
    #         else:
    #             raise NotImplementedError

    #     if is_dumped:
    #         if 'contains' in triplets['relationship'].unique():
    #             # New names found. Creating new entities.
    #             self._create_entities_food()
    #             self._create_entities_chemical()

    #     def _add_triplets(row):
    #         # nonlocal names_not_in_lut
    #         # nonlocal idxs_triplet_not_added
    #         nonlocal triplets_new_rows, mdata_new_rows

    #         if row['relationship'] == 'contains':
    #             head_type = 'food'
    #             tail_type = 'chemical'
    #             head_ids = self._link_name_to_entities(row['head'], head_type)
    #             tail_ids = self._link_name_to_entities(row['tail'], tail_type)
    #             if not head_ids or not tail_ids:
    #                 print(row['head'])
    #                 print(row['tail'])
    #                 raise ValueError("Something wrong here dude.")
    #             triplets_new_rows_, mdata_new_row_ \
    #                 = self._get_updated_triplets(head_ids, 'r1', tail_ids, row)
    #             triplets_new_rows += triplets_new_rows_
    #             mdata_new_rows += [mdata_new_row_]
    #         else:
    #             raise NotImplementedError

    #     # Keep track on the triplet adding outcome.
    #     # idxs_triplet_not_added = []
    #     # names_not_in_lut = []
    #     triplets_new_rows = []
    #     mdata_new_rows = []
    #     triplets.apply(_add_triplets, axis=1)

    #     def _expand_mdata(group):
    #         if group['_rid'].iloc[0] == 'r1':
    #             # Update tids for the old metadata that are extracted from Lit2KG. We do
    #             # not want to change metadata from other sources because they are not
    #             # matched based on names, but on the external IDs.
    #             # mdata_lit2kg \
    #             #     = self._mdata_contains.loc[
    #             #         self._mdata_contains['source'] == 'lit2kg:gpt-4'
    #             #     ]
    #             # mdata_lit2kg['tids'] = mdata_lit2kg.apply(
    #             #     lambda row: self.get_triplets(
    #             #         row['food_name'], 'contains', row['chemical_name'],
    #             #     ),
    #             #     axis=1,
    #             # )
    #             self._mdata_contains = pd.concat(
    #                 [
    #                     self._mdata_contains.astype('object'),
    #                     group.drop(columns=['_rid']).astype('object'),
    #                 ],
    #                 ignore_index=True,
    #             )
    #         else:
    #             raise NotImplementedError

    #     # Update the knowledge graph using the "hit" (found in lut) triplets.
    #     if triplets_new_rows:
    #         triplets_new = pd.DataFrame(triplets_new_rows)
    #         self._triplets = pd.concat(
    #             [self._triplets, triplets_new],
    #             ignore_index=True,
    #         )
    #         self._triplets['metadata_ids'] = self._triplets.apply(
    #             lambda row: self._lut_triplets[
    #                 f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}"
    #             ],
    #             axis=1,
    #         )
    #         self._load_lut_triplets()

    #         mdata_new = pd.DataFrame(mdata_new_rows)
    #         mdata_new.groupby('_rid').apply(_expand_mdata)

    #         self._triplets.to_csv(
    #             f"{self.path_output_dir}/triplets.tsv",
    #             sep='\t',
    #             index=False,
    #         )
    #         self._mdata_contains.to_csv(
    #             f"{self.path_output_dir}/mdata_contains.tsv",
    #             sep='\t',
    #             index=False,
    #         )
    #         print(f"# of new triplets added: {len(triplets_new)}")
    #         print(f"# of metadata entries added: {len(mdata_new)}")

        # Update the old metadata.

        # # Deal with the "miss" triplets.
        # if idxs_triplet_not_added:
        #     triplets_not_added = triplets.loc[idxs_triplet_not_added]
        #     names_not_in_lut = pd.DataFrame(names_not_in_lut).drop_duplicates()

        #     triplets_not_added.to_csv(
        #         f"{self.path_output_dir}/triplets_not_added.tsv",
        #         sep='\t',
        #         index=False,
        #     )
        #     names_not_in_lut.groupby('entity_type').apply(
        #         lambda group: group.drop(columns=['entity_type']).to_csv(
        #             f"{self.path_output_dir}/"
        #             f"_names_not_in_lut_{group['entity_type'].iloc[0]}.txt",
        #             sep='\t',
        #             index=False,
        #             header=False,
        #         )
        #     )
        #     print(
        #         f"{len(triplets_not_added)} triplets not added due to missing "
        #         "entities in the lookup tables. The file stored at "
        #         f"{self.path_output_dir}. Please retrieve the corresponding primary IDs"
        #         "to proceed."
        #     )

    def _get_new_names(
        self,
        entity_type,
        names,
    ):
        """
        """
        names_not_in_lut = []
        for name in names:
            if not self.luts.get_entity_ids(entity_type, name):
                names_not_in_lut += [name]

        return names_not_in_lut

    def _add_triplets_from_metadata_contains(
        self,
        metadata: pd.DataFrame,
    ):
        """
        """
        food_names_not_in_lut = self._get_new_names(
            'food', metadata['_food_name'].unique()
        )
        chemical_names_not_in_lut = self._get_new_names(
            'chemical', metadata['_chemical_name'].unique()
        )

        if food_names_not_in_lut:
            # Query database.
            # Create new entities.
            pass

        if chemical_names_not_in_lut:
            # Query database.
            # Create new entities.
            pass

        # Create new metadata.
        metadata = self.metadata.create(metadata)

        # Update existing and create new triplets.
        metadata_exploded = metadata.copy()
        metadata_exploded['head_id'] = metadata_exploded['_food_name'].apply(
            lambda x: self.luts.get_entity_ids('food', x)
        )
        metadata_exploded['tail_id'] = metadata_exploded['_chemical_name'].apply(
            lambda x: self.luts.get_entity_ids('chemical', x)
        )
        metadata_exploded = metadata_exploded.explode('head_id').explode('tail_id')
        metadata_exploded['relationship_id'] = 'r1'
        triplets = self.triplets.create(metadata_exploded)

        print(f"# metadata entries added: {len(metadata)}")
        print(f"# triplets added: {len(triplets)}")

    def add_triplets_from_metadata(
        self,
        metadata: pd.DataFrame,
        relationship_type: str = 'contains',
    ):
        """Basic operation of the knowledge graph. This takes a metadata dataframe and
        update lookup tables, entities, triplets, and metadata files.

        """
        # Sanity checks.
        # for col in ['head', 'relationship', 'tail']:
        #     if col not in triplets.columns:
        #         raise ValueError(f"Column '{col}' not found in triplets")
        if relationship_type == 'contains':
            self._add_triplets_from_metadata_contains(metadata)
        else:
            raise NotImplementedError

        # Dump new files.
        if self.path_output_dir:
            self._save()

        # for relationship in triplets['relationship'].unique():
        #     if relationship == 'contains':
        #         is_dumped = self._dump_new_names_contains(triplets)
        #     else:
        #         raise NotImplementedError

        # if is_dumped:
        #     if 'contains' in triplets['relationship'].unique():
        #         # New names found. Creating new entities.
        #         self._create_entities_food()
        #         self._create_entities_chemical()

        # def _add_triplets(row):
        #     # nonlocal names_not_in_lut
        #     # nonlocal idxs_triplet_not_added
        #     nonlocal triplets_new_rows, mdata_new_rows

        #     if row['relationship'] == 'contains':
        #         head_type = 'food'
        #         tail_type = 'chemical'
        #         head_ids = self._link_name_to_entities(row['head'], head_type)
        #         tail_ids = self._link_name_to_entities(row['tail'], tail_type)
        #         if not head_ids or not tail_ids:
        #             print(row['head'])
        #             print(row['tail'])
        #             raise ValueError("Something wrong here dude.")
        #         triplets_new_rows_, mdata_new_row_ \
        #             = self._get_updated_triplets(head_ids, 'r1', tail_ids, row)
        #         triplets_new_rows += triplets_new_rows_
        #         mdata_new_rows += [mdata_new_row_]
        #     else:
        #         raise NotImplementedError

        # # Keep track on the triplet adding outcome.
        # # idxs_triplet_not_added = []
        # # names_not_in_lut = []
        # triplets_new_rows = []
        # mdata_new_rows = []
        # triplets.apply(_add_triplets, axis=1)

        # def _expand_mdata(group):
        #     if group['_rid'].iloc[0] == 'r1':
        #         # Update tids for the old metadata that are extracted from Lit2KG. We do
        #         # not want to change metadata from other sources because they are not
        #         # matched based on names, but on the external IDs.
        #         # mdata_lit2kg \
        #         #     = self._mdata_contains.loc[
        #         #         self._mdata_contains['source'] == 'lit2kg:gpt-4'
        #         #     ]
        #         # mdata_lit2kg['tids'] = mdata_lit2kg.apply(
        #         #     lambda row: self.get_triplets(
        #         #         row['food_name'], 'contains', row['chemical_name'],
        #         #     ),
        #         #     axis=1,
        #         # )
        #         self._mdata_contains = pd.concat(
        #             [
        #                 self._mdata_contains.astype('object'),
        #                 group.drop(columns=['_rid']).astype('object'),
        #             ],
        #             ignore_index=True,
        #         )
        #     else:
        #         raise NotImplementedError

        # # Update the knowledge graph using the "hit" (found in lut) triplets.
        # if triplets_new_rows:
        #     triplets_new = pd.DataFrame(triplets_new_rows)
        #     self._triplets = pd.concat(
        #         [self._triplets, triplets_new],
        #         ignore_index=True,
        #     )
        #     self._triplets['metadata_ids'] = self._triplets.apply(
        #         lambda row: self._lut_triplets[
        #             f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}"
        #         ],
        #         axis=1,
        #     )
        #     self._load_lut_triplets()

        #     mdata_new = pd.DataFrame(mdata_new_rows)
        #     mdata_new.groupby('_rid').apply(_expand_mdata)

        #     self._triplets.to_csv(
        #         f"{self.path_output_dir}/triplets.tsv",
        #         sep='\t',
        #         index=False,
        #     )
        #     self._mdata_contains.to_csv(
        #         f"{self.path_output_dir}/mdata_contains.tsv",
        #         sep='\t',
        #         index=False,
        #     )
        #     print(f"# of new triplets added: {len(triplets_new)}")
        #     print(f"# of metadata entries added: {len(mdata_new)}")
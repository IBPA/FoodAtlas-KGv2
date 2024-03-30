from ast import literal_eval
from collections import OrderedDict
import re

import pandas as pd
from inflection import singularize, pluralize
from tqdm import tqdm

from .utils import constants
from ._query import query_ncbi_taxonomy, query_pubchem_compound

def group_synonyms(synonyms_groups: list[list[str]]):
    """A list of synonym groups, each representing singluar and plural forms of a word.
    This function groups the synonyms no group is a subset is another group.
    For example:
    ```
    INPUT = [
        ['apple', 'apples'],
        ['apples', 'apple'],
        ['person', 'people', 'peoples'],
        ['people', 'peoples'],
    ]

    OUTPUT = [
        ['apple', 'apples'],
        ['person', 'people', 'peoples'],
    ]
    ```

    TODO: Currently O(n^2 * k) where n is the number of groups and k is the number of
    synonyms in a group. Potential optimization?

    """
    print("Grouping synonyms...")
    synonyms_groups = [set(synonyms) for synonyms in synonyms_groups]
    synonyms_groups = sorted(synonyms_groups, key=lambda x: len(x))

    to_remove = [False for _ in range(len(synonyms_groups))]
    for i in tqdm(range(len(synonyms_groups)), total=len(synonyms_groups)):
        if to_remove[i]:
            continue
        for j in range(i + 1, len(synonyms_groups)):
            if synonyms_groups[i].issubset(synonyms_groups[j]):
                to_remove[i] = True
                break

    result = []
    for i in range(len(synonyms_groups)):
        if not to_remove[i]:
            result += [list(synonyms_groups[i])]

    return result


class Entities:
    """Class for managing entities in the knowledge graph.

    Arritbutes:
        COLUMNS (list[str]): Columns of the entities DataFrame.
        FAID_PREFIX (str): Prefix for the FoodAtlas ID.

    Args:
        path_entities (str): Path to the entities file.
        path_lut_food (str): Path to the lookup table for food.
        path_lut_chemical (str): Path to the lookup table for chemical.
        path_cache_dir (str): Path to the cache directory.

    """
    COLUMNS = [
        'foodatlas_id',
        'entity_type',
        'common_name',
        'scientific_name',
        'synonyms',
        'external_ids',
    ]
    FAID_PREFIX = 'e'

    def __init__(
        self,
        path_entities: str,
        path_lut_food: str,
        path_lut_chemical: str,
        path_kg: str = None,
        path_cache_dir: str = None,
    ):
        self.path_entities = path_entities
        self.path_lut_food = path_lut_food
        self.path_lut_chemical = path_lut_chemical
        self.path_kg = path_kg
        self.path_cache_dir = path_cache_dir

        self._load()

    def _load(self):
        """Helper for loading.

        """
        # Load entities.
        self._entities = pd.read_csv(
            self.path_entities,
            sep='\t',
            converters={
                'synonyms': literal_eval,
                'external_ids': literal_eval,
            },
        ).set_index('foodatlas_id')

        # Load lookup tables.
        luts = []
        for path_lut in [self.path_lut_food, self.path_lut_chemical]:
            lut_df = pd.read_csv(
                path_lut,
                sep='\t',
                converters={
                    'foodatlas_id': lambda x: literal_eval(x),
                    'name': str,
                },
            )
            lut = dict(zip(lut_df['name'], lut_df['foodatlas_id']))
            luts += [lut]

        self._lut_food = luts[0]
        self._lut_chemical = luts[1]

        # Get the current entity ID.
        eid = self._entities.index.str.slice(1).astype(int).max()
        self._curr_eid = eid + 1 if pd.notna(eid) else 1

    def _save(
        self,
        path_output_dir: str,
    ):
        """Helper for saving.

        Args:
            path_output_dir (str): Path to the output directory.

        """
        self._entities.to_csv(
            f"{path_output_dir}/entities.tsv", sep='\t',
        )
        pd.DataFrame(
            self._lut_food.items(), columns=['name', 'foodatlas_id']
        ).to_csv(
            f"{path_output_dir}/lookup_table_food.tsv", sep='\t', index=False
        )
        pd.DataFrame(
            self._lut_chemical.items(), columns=['name', 'foodatlas_id']
        ).to_csv(
            f"{path_output_dir}/lookup_table_chemical.tsv", sep='\t', index=False
        )

    def _create_food_entities_from_ncbi_taxonomy(
        self,
        records: pd.DataFrame,
    ):
        """Helper for creating food entities, which have NCBI Taxonomy IDs.

        Args:
            records (pd.DataFrame): Records from NCBI Taxonomy.

        """
        def _parse_names(row):
            scientific_name = row['ScientificName']
            other_names = row['OtherNames']

            synonyms_scientific = [scientific_name]
            synonyms_common = []
            synonyms_others = []
            if row['OtherNames'] is not None:
                synonyms_scientific += other_names['Synonym']
                synonyms_scientific += other_names['EquivalentName']
                if other_names['Name']:
                    for name in other_names['Name']:
                        if name['ClassCDE'] in ['misspelling', 'authority']:
                            synonyms_scientific += [name['DispName']]

                synonyms_common += other_names['CommonName']
                if 'GenbankCommonName' in other_names:
                    synonyms_common += [other_names['GenbankCommonName']]
                if 'BlastName' in other_names:
                    synonyms_common += [other_names['BlastName']]

                synonyms_others += other_names['Includes']

            row['scientific_name'] = scientific_name.strip().lower()

            if synonyms_common:
                synonyms_common_sp = []
                for name in synonyms_common:
                    synonyms_common_sp += [singularize(name), pluralize(name)]
                synonyms_common += synonyms_common_sp
                row['common_name'] = min(synonyms_common, key=len).strip().lower()
            else:
                row['common_name'] = row['scientific_name']

            # Include abbreviations for scientific names.
            if len(scientific_name.split(' ')) > 1:
                # Consider only species or lower.
                synonyms_scientific_abbr = []
                for name in synonyms_scientific:
                    terms = name.split(' ')
                    terms[0] = terms[0][0] + '.'
                    synonyms_scientific_abbr += [' '.join(terms)]
                synonyms_scientific += synonyms_scientific_abbr

            synonyms = synonyms_common + synonyms_others + synonyms_scientific
            synonyms = list(OrderedDict.fromkeys(synonyms).keys())
            synonyms = [x.strip().lower() for x in synonyms]
            row['synonyms'] = synonyms + [constants.get_lookup_key_by_id(
                'ncbi_taxon_id', row['TaxId']
            )]

            return row

        entities_new = records.copy()
        entities_new[self.COLUMNS] = None
        entities_new = entities_new.apply(_parse_names, axis=1)
        entities_new = entities_new.rename(columns={'TaxId': 'ncbi_taxon_id'})
        entities_new['external_ids'] \
            = entities_new['ncbi_taxon_id'].apply(
                lambda x: {'ncbi_taxon_id': x}
            )

        entities_new['foodatlas_id'] = [
            f"e{i}" for i in range(self._curr_eid, self._curr_eid + len(entities_new))
        ]
        self._curr_eid += len(entities_new)

        entities_new['entity_type'] = 'food'
        entities_new = entities_new[self.COLUMNS].set_index('foodatlas_id')
        self._entities = pd.concat([self._entities, entities_new])
        self._update_lut(entities_new)

    def _create_food_entities_from_synonym_groups(
        self,
        synonym_groups: list[list[str]],
    ):
        """Helper for creating food entities, which do not have NCBI Taxonomy IDs.

        Args:
            synonym_groups (list[list[str]]): Groups of synonyms.

        """
        entities_new_rows = []
        for synonyms in synonym_groups:
            found = False
            for name in synonyms:
                if self.get_entity_ids('food', name):
                    found = True
                    break

            if not found:
                entities_new_rows += [{
                    'foodatlas_id': f"e{self._curr_eid}",
                    'entity_type': 'food',
                    'common_name': min(synonyms, key=len),
                    'scientific_name': '',
                    'synonyms': synonyms,
                    'external_ids': {},
                }]
                self._curr_eid += 1

        entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
        self._entities = pd.concat([self._entities, entities_new])
        self._update_lut(entities_new)

    def _create_food_entities(
        self,
        entity_names_new: list[str],
    ):
        """Helper for creating food entities. For food entity, we first query
        NCBI Taxonomy to see if there is an ID. If not, we create food entities without
        NCBI Taxonomy IDs.

        Args:
            entity_names_new (list[str]): Names of the entities.

        """
        # Step 0. Create synonyms by applying singularize and pluralize.
        entity_synonyms = [
            list(set([name, singularize(name), pluralize(name)]))
            for name in entity_names_new
        ]
        entity_names_all \
            = list(set([name for names in entity_synonyms for name in names]))

        # Step 1. Query NCBI Taxonomy to see if there is an ID.
        records_ncbi_taxonomy = query_ncbi_taxonomy(
            entity_names_all,
            self.path_kg,
            self.path_cache_dir,
        )
        self._create_food_entities_from_ncbi_taxonomy(records_ncbi_taxonomy)

        # Step 2. Create entities that are still new to the KG.
        entity_synonyms_grouped = group_synonyms(entity_synonyms)
        self._create_food_entities_from_synonym_groups(entity_synonyms_grouped)

        # Step 3. Link all names to entities and update synonyms & lookup table.
        for synonyms in entity_synonyms_grouped:
            found = False
            for name in synonyms:
                eids = self.get_entity_ids('food', name)
                for eid in eids:
                    self._update_entity_synonyms(eid, synonyms)
                    found = True
            if not found:
                raise ValueError(f"Entity not found for synonyms: {synonyms}")


    def _create_chemical_entities_from_pubchem_compound(
        self,
        records: pd.DataFrame,
    ):
        """Helper for creating chemical entities, which contain PubChem CIDs.

        Args:
            records (pd.DataFrame): Records from PubChem Compound.

        """
        def _parse_names(row):
            row['scientific_name'] = row['IUPACName'].strip().lower() \
                if not pd.isna(row['IUPACName']) else None

            row['synonyms'] = [s.strip().lower() for s in row['SynonymList']]
            row['synonyms'] += [row['scientific_name']] \
                if (
                    row['scientific_name']
                    and row['scientific_name'] not in row['synonyms']
                ) \
                else []
            row['synonyms'] += [constants.get_lookup_key_by_id(
                'pubchem_cid', row['CID']
            )]
            row['common_name'] = row['synonyms'][0]
            # # Pick a common name that is not all digits.
            # synonyms_all_digits = [
            #     True if re.match('[\d-]+$', s) else False for s in row['synonyms']
            # ]
            # if all(synonyms_all_digits):
            #     row['common_name'] = row['scientific_name']
            # else:
            #     row['common_name'] = row['synonyms'][synonyms_all_digits.index(False)]

            return row

        entities_new = records.copy()
        entities_new[self.COLUMNS] = None
        entities_new = entities_new.apply(_parse_names, axis=1)
        entities_new['external_ids'] \
            = entities_new['CID'].apply(
                lambda x: {'pubchem_cid': x}
            )

        entities_new['foodatlas_id'] = [
            f"e{i}" for i in range(self._curr_eid, self._curr_eid + len(entities_new))
        ]
        self._curr_eid += len(entities_new)

        entities_new['entity_type'] = 'chemical'
        entities_new = entities_new[self.COLUMNS].set_index('foodatlas_id')
        self._entities = pd.concat([self._entities, entities_new])
        self._update_lut(entities_new)

    def _create_chemical_entities_from_names(
        self,
        names: list[str],
    ):
        """Helper for creating chemical entities, which do not have PubChem CIDs.

        Args:
            names (list[str]): Names of the entities.

        """
        entities_new_rows = []
        for name in names:
            if not self.get_entity_ids('chemical', name):
                entities_new_rows += [{
                    'foodatlas_id': f"e{self._curr_eid}",
                    'entity_type': 'chemical',
                    'common_name': name,
                    'scientific_name': None,
                    'synonyms': [name],
                    'external_ids': {},
                }]
                self._curr_eid += 1

        entities_new = pd.DataFrame(entities_new_rows).set_index('foodatlas_id')
        self._entities = pd.concat([self._entities, entities_new])
        self._update_lut(entities_new)

    def _create_chemical_entities(
        self,
        entity_names_new: list[str],
    ):
        """Helper for creating chemical entities. For chemical entity, we first query
        PubChem Compound to see if there is an ID. If not, we create chemical entities
        without PubChem CIDs.

        Args:
            entity_names_new (list[str]): Names of the entities.

        """
        # Step 1. Query PubChem Compound to see if there is an ID.
        records_pubchem_compound = query_pubchem_compound(
            entity_names_new,
            self.path_kg,
            self.path_cache_dir,
        )
        self._create_chemical_entities_from_pubchem_compound(records_pubchem_compound)

        # Step 2. Create entities that are still new to the KG.
        self._create_chemical_entities_from_names(entity_names_new)

    def _update_entity_synonyms(
        self,
        entity_id: str,
        synonyms_new: list[str],
    ):
        """Helper for updating synonyms of an entity, which also update the lookup
        table.

        Args:
            entity_id (str): FAID.
            synonyms_new (list[str]): New synonyms.

        """
        entity = self.get_entity(entity_id)
        synonyms = set(entity['synonyms'])
        updated = False
        for synonym in synonyms_new:
            if synonym not in synonyms:
                synonyms.add(synonym)
                if synonym not in self._lut_food:
                    self._lut_food[synonym] = []
                self._lut_food[synonym] += [entity_id]

                updated = True

        if updated:
            self._entities.at[entity_id, 'synonyms'] = list(synonyms)
            self._update_lut(self._entities.loc[[entity_id]])

    def _update_lut(
        self,
        entities: pd.DataFrame,
    ):
        """Helper for updating lookup tables.

        Args:
            entities (pd.DataFrame): Entities to be updated.

        """
        def _add_to_lut(row):
            if row['entity_type'] == 'food':
                lut = self._lut_food
            elif row['entity_type'] == 'chemical':
                lut = self._lut_chemical
            else:
                raise ValueError(f"Invalid entity type: {row['entity_type']}")

            # Add synonyms to the lookup table.
            for synonym in row['synonyms']:
                if synonym not in lut:
                    lut[synonym] = []
                lut[synonym] += [row.name]
                lut[synonym] = list(set(lut[synonym]))

            # # Add external IDs to the lookup table.
            # if row['external_ids']:
            #     for name, ids in row['external_ids'].items():
            #         if not isinstance(ids, list):
            #             ids = [ids]
            #         for id_ in ids:
            #             key_lut = constants.get_lookup_key_by_id(name, id_)

            #             if key_lut not in lut:
            #                 lut[key_lut] = []
            #             lut[key_lut] += [row.name]

        entities.apply(_add_to_lut, axis=1)

    def create(
        self,
        entity_type: str,
        entity_names_new: list[str],
    ):
        """Create new entities.

        Args:
            entity_type (str): Type of the entity.
            entity_names_new (list[str]): Names of the entities.

        """
        if entity_type == 'food':
            self._create_food_entities(entity_names_new)
        elif entity_type == 'chemical':
            self._create_chemical_entities(entity_names_new)
        else:
            raise ValueError(f"Invalid entity type: {entity_type}.")

    def get_entity_ids(
        self,
        entity_type: str,
        entity_name: str,
    ) -> list[str]:
        """Get FAID for an entity with a name.

        Args:
            entity_type (str): Type of the entity.
            entity_name (str): Name of the entity.

        Returns:
            list[str]: List of FAIDs.

        """
        if entity_type == 'food':
            lut = self._lut_food
        elif entity_type == 'chemical':
            lut = self._lut_chemical
        else:
            raise ValueError(f'Invalid entity_type: {entity_type}')

        return lut[entity_name] if entity_name in lut else []

    def get_entity(
        self,
        entity_id: str,
    ) -> pd.Series:
        """Get an entity by FAID.

        Args:
            entity_id (str): FAID.

        Returns:
            pd.Series: Entity.

        """
        return self._entities.loc[entity_id]

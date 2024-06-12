# -*- coding: utf-8 -*-
"""

Knowledge Graph Class.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

Todo:
    * disambiguate function can be improved.

"""
import logging
import warnings

import pandas as pd
from pympler import asizeof
from tqdm import tqdm

from .entities import Entities
from ._metadata import Metadata
from ._triplets import Triplets

logger = logging.getLogger(__name__)
tqdm.pandas()

class KnowledgeGraph:
    """Class to represent the knowledge graph.

    Args:
        path_kg (str, optional): The path to the knowledge graph. Defaults to
            "outputs/kg".
        path_cache_dir ([type], optional): The path to the cached directory. Defaults
            to "outputs/kg/_cache".

    """

    def __init__(
        self,
        path_kg="outputs/kg",
        path_cache_dir="outputs/kg/_cache",
    ):
        self.path_kg = path_kg
        self.path_cache_dir = path_cache_dir
        self._load()
        self.print_stats()

    def _load(self):
        """Load the knowledge graph data from the `self.path_kg`.

        """
        logger.info("Start loading the knowledge graph...")

        self.metadata = Metadata(
            path_metadata_contains=f"{self.path_kg}/metadata_contains.tsv",
        )
        self.triplets = Triplets(
            path_triplets=f"{self.path_kg}/triplets.tsv",
        )
        self.entities = Entities(
            path_entities=f"{self.path_kg}/entities.tsv",
            path_lut_food=f"{self.path_kg}/lookup_table_food.tsv",
            path_lut_chemical=f"{self.path_kg}/lookup_table_chemical.tsv",
            path_kg=self.path_kg,
            path_cache_dir=self.path_cache_dir,
        )

        logger.info("Completed loading the knowledge graph!")

    def save(self, path_output_dir=None):
        """Helper function to save the knowledge graph data.

        """
        self.metadata._save(path_output_dir)
        self.triplets._save(path_output_dir)
        self.entities._save(path_output_dir)

    def print_stats(self):
        """Print the statistics of the knowledge graph.

        """
        logger.info(
            f"KG space consumption: {asizeof.asizeof(self) / 1024 / 1024:.2f} MB"
        )

    def _add_triplets_from_metadata_contains(
        self,
        metadata: pd.DataFrame,
    ):
        """Helper function to add triplets from `contains` metadata.

        Args:
            metadata (pd.DataFrame): The metadata dataframe.

        """
        food_names_not_in_lut = self.entities.get_new_names(
            'food', metadata['_food_name'].unique()
        )
        chemical_names_not_in_lut = self.entities.get_new_names(
            'chemical', metadata['_chemical_name'].unique()
        )

        if food_names_not_in_lut:
            self.entities.create('food', food_names_not_in_lut)

        if chemical_names_not_in_lut:
            self.entities.create('chemical', chemical_names_not_in_lut)

        self._disambiguate_synonyms()

        # Create new metadata.
        metadata = self.metadata.create(metadata)

        # Update existing and create new triplets.
        metadata_exploded = metadata.copy()
        metadata_exploded['head_id'] = metadata_exploded['_food_name'].apply(
            lambda x: self.entities.get_entity_ids('food', x)
        )
        metadata_exploded['tail_id'] = metadata_exploded['_chemical_name'].apply(
            lambda x: self.entities.get_entity_ids('chemical', x)
        )
        metadata_exploded = metadata_exploded.explode('head_id').explode('tail_id')
        metadata_exploded['relationship_id'] = 'r1'
        triplets = self.triplets.create(metadata_exploded)

        logger.info(f"# metadata entries added: {len(metadata)}")
        logger.info(f"# triplets added: {len(triplets)}")

    def _disambiguate_synonyms(self):
        """Make sure every synonym is uniquely linked one entity.

        """
        logger.info("Start disambiguating synonyms...")

        def _remove_entity_synonyms(eid: str, synonyms_to_remove: list[str]):
            entities.at[eid, 'synonyms'] = [
                x for x in entities.loc[eid, 'synonyms'] if x not in synonyms_to_remove
            ]
            synonyms = entities.loc[eid, 'synonyms']

            if not synonyms:
                warnings.warn(
                    f"{entities.loc[eid]}: Synonyms are empty."
                )
            else:
                if entities.at[eid, 'common_name'] not in synonyms:
                    entities.at[eid, 'common_name'] = synonyms[0]

                if entities.at[eid, 'scientific_name'] not in synonyms:
                    entities.at[eid, 'scientific_name'] = ''

        entities = self.entities._entities

        placeholders_rows = []
        for entity_type in ['food', 'chemical']:
            lut = eval(f"self.entities._lut_{entity_type}")
            for name, eids, in lut.items():
                if len(eids) == 1:
                    continue

                if '_placeholder_to' in entities.at[eids[0], 'external_ids']:
                    # Already a placeholder entity.
                    # 1. Add the eids to the existing placeholder entity.
                    entities.at[eids[0], 'external_ids']['_placeholder_to'] += eids
                    # 2. Update the corresponding non-placeholders.
                    for eid in eids[1:]:
                        _remove_entity_synonyms(eid, [name])
                        if '_placeholder_from' not in entities.at[eid, 'external_ids']:
                            entities.at[eid, 'external_ids']['_placeholder_from'] = []
                        entities.at[eid, 'external_ids']['_placeholder_from'] \
                            += [eids[0]]
                        entities.at[eid, 'external_ids']['_placeholder_from'] \
                            = sorted(set(
                                entities.at[eid, 'external_ids']['_placeholder_from']
                            ))
                    lut[name] = [eids[0]]
                else:
                    # Create a new placeholder entity.
                    placeholders_rows += [{
                        'foodatlas_id': f"e{self.entities._curr_eid}",
                        'entity_type': entity_type,
                        'common_name': name,
                        'synonyms': [name],
                        'external_ids': {'_placeholder_to': eids},
                    }]
                    self.entities._curr_eid += 1

        if not placeholders_rows:
            return

        placeholders = pd.DataFrame(
            placeholders_rows
        ).set_index('foodatlas_id', drop=True)
        self.entities._entities = pd.concat(
            [self.entities._entities, placeholders]
        )

        def _update_entity_synonyms(row):
            eids_to_update = row['external_ids']['_placeholder_to']

            for eid in eids_to_update:
                entities = self.entities._entities
                entities.at[eid, 'synonyms'] = [
                    x for x in entities.loc[eid, 'synonyms'] if x not in row['synonyms']
                ]

                if not entities.at[eid, 'synonyms']:
                    warnings.warn(
                        f"{entities.loc[eid]}: Synonyms are empty."
                    )
                else:
                    if entities.at[eid, 'common_name'] \
                            not in entities.at[eid, 'synonyms']:
                        entities.at[eid, 'common_name'] \
                            = entities.at[eid, 'synonyms'][0]

                    if entities.at[eid, 'scientific_name'] \
                            not in entities.at[eid, 'synonyms']:
                        entities.at[eid, 'scientific_name'] = ''

                if '_placeholder_from' not in entities.at[eid, 'external_ids']:
                    entities.at[eid, 'external_ids']['_placeholder_from'] = []
                entities.at[eid, 'external_ids']['_placeholder_from'] += [row.name]

        placeholders.apply(_update_entity_synonyms, axis=1)

        def _update_lut_placeholders(row):
            nonlocal self

            lut = eval(f"self.entities._lut_{row['entity_type']}")
            synonyms = row['synonyms']
            for synonym in synonyms:
                lut[synonym] = [row.name]

        placeholders.apply(_update_lut_placeholders, axis=1)

        logger.info("Completed disambiguating synonyms!")

    def merge_entities(self):
        pass

    def remove_entities(self):
        pass

    def add_triplets_from_metadata(
        self,
        metadata: pd.DataFrame,
        relationship_type: str = 'contains',
    ):
        """Basic operation of the knowledge graph. This takes a metadata dataframe and
        update lookup tables, entities, triplets, and metadata files.

        Args:
            metadata (pd.DataFrame): The metadata dataframe.
            relationship_type (str, optional): The relationship type. Defaults to
                'contains'.

        """
        if relationship_type == 'contains':
            self._add_triplets_from_metadata_contains(metadata)
        else:
            raise NotImplementedError

    def get_triplets(
        self,
        head_id: str = None,
        tail_id: str = None,
    ) -> pd.DataFrame:
        """Get triplets.

        Args:
            head_id (str, optional): The head id. None is considered as wildcart.
                Defaults to None.
            tail_id (str, optional): The head id. None is considered as wildcart.
                Defaults to None.

        Returns:
            pd.DataFrame: Triplets.

        """
        _triplets = self.triplets._triplets
        if head_id is not None:
            _triplets = _triplets[_triplets['head_id'] == head_id]
        if tail_id is not None:
            _triplets = _triplets[_triplets['tail_id'] == tail_id]

        def _get_metadata_df(row):
            nonlocal _metadata_dfs

            metadata_df = self.metadata.get(row['metadata_ids']).copy()
            metadata_df['triplet_id'] = row.name
            _metadata_dfs += [metadata_df]

        _metadata_dfs = []
        _triplets.apply(_get_metadata_df, axis=1)

        return pd.concat(_metadata_dfs) if _metadata_dfs else pd.DataFrame()

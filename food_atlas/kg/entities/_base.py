import logging
from ast import literal_eval
from collections import OrderedDict

import pandas as pd

from ._food import create_food_entities
from ._chemical import create_chemical_entities

logger = logging.getLogger(__name__)


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
        synonyms = OrderedDict.fromkeys(entity['synonyms'])
        updated = False
        for synonym in synonyms_new:
            if synonym not in synonyms:
                # synonyms.add(synonym)
                synonyms[synonym] = None
                if synonym not in self._lut_food:
                    self._lut_food[synonym] = []
                self._lut_food[synonym] += [entity_id]

                updated = True

        if updated:
            self._entities.at[entity_id, 'synonyms'] = list(synonyms.keys())
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
                lut[synonym] += [row.name] if row.name not in lut[synonym] else []

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
            create_food_entities(self, entity_names_new)
        elif entity_type == 'chemical':
            create_chemical_entities(self, entity_names_new)
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

    def get_new_names(
        self,
        entity_type: str,
        names: list[str],
    ) -> list[str]:
        """Get the names that are not in the lookup table.

        Args:
            entity_type (str): The entity type.
            names (list[str]): The list of names.

        Returns:
            list[str]: The list of names that are not in the lookup table.

        """
        n_found = 0
        names_not_in_lut = []
        for name in names:
            if not self.get_entity_ids(entity_type, name):
                names_not_in_lut += [name]
            else:
                n_found += 1

        logger.info(
            f"# of unique {entity_type} name existing/new: "
            f"{n_found}/{len(names_not_in_lut)}"
        )

        return names_not_in_lut

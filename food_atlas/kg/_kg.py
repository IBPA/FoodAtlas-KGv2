import pandas as pd
from pympler import asizeof

from ._metadata import Metadata
from ._triplets import Triplets
from ._entities import Entities


class KnowledgeGraph:
    """Class to represent the knowledge graph.

    Args:
        path_kg (str, optional): The path to the knowledge graph. Defaults to
            "outputs/kg".
        path_output_dir ([type], optional): The path to the output directory. Defaults
            to None.

    """

    def __init__(
        self,
        path_kg="outputs/kg",
        path_output_dir=None,
    ):
        self.path_kg = path_kg
        self.path_output_dir = path_output_dir
        self._load()
        self.print_stats()

    def _load(self):
        """Load the knowledge graph data from the `self.path_kg`.

        """
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
            path_cache_dir=self.path_output_dir,
        )

    def _save(self):
        """Helper function to save the knowledge graph data.

        """
        self.metadata._save(self.path_output_dir)
        self.triplets._save(self.path_output_dir)
        self.entities._save(self.path_output_dir)

    def print_stats(self):
        """Print the statistics of the knowledge graph.

        """
        print(f"KG space consumption: {asizeof.asizeof(self) / 1024 / 1024:.2f} MB")

    def _get_new_names(
        self,
        entity_type: str,
        names: list[str],
    ) -> list[str]:
        """Helper function to get the names that are not in the lookup table.

        Args:
            entity_type (str): The entity type.
            names (list[str]): The list of names.

        Returns:
            list[str]: The list of names that are not in the lookup table.

        """
        n_found = 0
        names_not_in_lut = []
        for name in names:
            if not self.entities.get_entity_ids(entity_type, name):
                names_not_in_lut += [name]
            else:
                n_found += 1

        print(
            f"# of unique {entity_type} name existing/new: "
            f"{n_found}/{len(names_not_in_lut)}"
        )

        return names_not_in_lut

    def _add_triplets_from_metadata_contains(
        self,
        metadata: pd.DataFrame,
    ):
        """Helper function to add triplets from `contains` metadata.

        Args:
            metadata (pd.DataFrame): The metadata dataframe.

        """
        food_names_not_in_lut = self._get_new_names(
            'food', metadata['_food_name'].unique()
        )
        chemical_names_not_in_lut = self._get_new_names(
            'chemical', metadata['_chemical_name'].unique()
        )

        if food_names_not_in_lut:
            self.entities.create('food', food_names_not_in_lut)

        if chemical_names_not_in_lut:
            self.entities.create('chemical', chemical_names_not_in_lut)

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

        print(f"# metadata entries added: {len(metadata)}")
        print(f"# triplets added: {len(triplets)}")

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

        # Dump new files.
        if self.path_output_dir:
            self._save()

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
            metadata_df['triple_id'] = row['foodatlas_id']
            _metadata_dfs += [metadata_df]

        _metadata_dfs = []
        _triplets.apply(_get_metadata_df, axis=1)

        return pd.concat(_metadata_dfs)

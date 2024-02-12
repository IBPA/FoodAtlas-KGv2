import os
from itertools import product

import pandas as pd
from pympler import asizeof

from .utils import load_entities, load_lookup_tables, load_mdata


class KnowledgeGraph:

    def __init__(
        self,
        path_kg="outputs/kg",
    ):
        self.path_kg = path_kg
        self._load_kg()

        # Create a new directory to store the current KG.
        self._timestamp = pd.Timestamp.now()
        os.makedirs(f"{self.path_kg}/{self._timestamp}")
        print(
            f"Created a new directory at {self.path_kg}/{self._timestamp} for the "
            "new KG."
        )

    def _load_kg(self):
        # Load the knowledge graph data.
        self._entities = load_entities(f"{self.path_kg}/entities.tsv")
        self._relations = pd.read_csv(f"{self.path_kg}/relationships.tsv", sep='\t')
        self._triplets = pd.read_csv(f"{self.path_kg}/triplets.tsv", sep='\t')

        # Load the metadata.
        self._mdata_contains = load_mdata()[0]

        # Load the lookup tables.
        self._lut_food, self._lut_chemical = load_lookup_tables()
        self._lut_triplets = dict(zip(
            self._triplets.apply(
                lambda row:
                    f"{row['head_id']}_{row['relationship_id']}_{row['tail_id']}",
                axis=1,
            ),
            self._triplets['foodatlas_id'],
        ))

        # Keep track of the current maximum entity IDs.
        self._curr_tid \
            = self._triplets['foodatlas_id'].str.slice(1).astype(int).max() + 1
        self._curr_mcid \
            = self._mdata_contains['foodatlas_id'].str.slice(2).astype(int).max() + 1

        print(f"KG space consumption: {asizeof.asizeof(self) / 1024 / 1024:.2f} MB")

    def get_x(self):
        pass

    def _merge_entity(self, entity_new, entity_id):
        pass

    def _create_entity(self, entity_new):
        pass

    def _get_updated_triplets(
        self,
        head_ids: list[str],
        relationship_id: str,
        tail_id: list[str],
        row: pd.Series,
    ) -> tuple[list[dict], dict]:
        """
        """
        triplets_new_rows = []
        mdata_new_row = {
            'foodatlas_id': None,
            'tids': [],
        }

        for head_id, tail_id in product(head_ids, tail_id):
            if f"{head_id}_{relationship_id}_{tail_id}" not in self._lut_triplets:
                self._lut_triplets[f"{head_id}_{relationship_id}_{tail_id}"] \
                    = f"t{self._curr_tid}"
                self._curr_tid += 1
            triplets_new_rows += [{
                'foodatlas_id':
                    self._lut_triplets[f"{head_id}_{relationship_id}_{tail_id}"],
                'head_id': head_id,
                'relationship_id': relationship_id,
                'tail_id': tail_id,
            }]
            mdata_new_row['tids'] += [
                self._lut_triplets[f"{head_id}_{relationship_id}_{tail_id}"]
            ]

        if relationship_id == 'r1':
            mdata_new_row.update({
                '_rid': 'r1',
                'foodatlas_id': f"mc{self._curr_mcid}",
                'food_name': row['head'],
                'chemical_name': row['tail'],
                'conc_value': row['conc_value'],  # TODO: Call parse_conc.
                'conc_unit': row['conc_unit'],  # TODO: Call parse_conc.
                'food_part': row['food_part'],  # TODO: Call parse_food_part.
                'food_processing': row['food_processing'],  # TODO: Call parse_food_processing.
                'source': row['source'],
                'reference': row['reference'],
                'quality_score': row['quality_score'],  # TODO: Call predict_quality.
                '_extracted_conc': row['_extracted_conc'],
                '_extracted_food_part': row['_extracted_food_part'],
            })
            self._curr_mcid += 1
        else:
            raise NotImplementedError

        return triplets_new_rows, mdata_new_row

    def _link_name_to_entities(
            self,
            entity_name: str,
            entity_type: str,
            ) -> list[str]:
        """Return the entity ID for the given entity name and type.

        Args:
            entity_name: The name of the entity.
            entity_type: The type of the entity.

        Returns:
            A list of foodatlas_ids for entities.

        """
        if entity_type == 'food':
            if entity_name in self._lut_food:
                return self._lut_food[entity_name]
        elif entity_type == 'chemical':
            if entity_name in self._lut_chemical:
                return self._lut_chemical[entity_name]
        else:
            raise NotImplementedError

        return []

    # def add_triplets(self, head_name, relation_id, tail_name, metadata):
    def add_triplets(
            self,
            triplets: pd.DataFrame,
            ):
        """
        """
        # Sanity checks.
        for col in ['head', 'relationship', 'tail']:
            if col not in triplets.columns:
                raise ValueError(f"Column '{col}' not found in triplets")

        def _add_triplets(row):
            nonlocal idxs_triplet_not_added, names_not_in_lut
            nonlocal triplets_new_rows, mdata_new_rows

            if row['relationship'] == 'contains':
                head_type = 'food'
                tail_type = 'chemical'
                head_ids = self._link_name_to_entities(row['head'], head_type)
                tail_ids = self._link_name_to_entities(row['tail'], tail_type)
                if not head_ids or not tail_ids:
                    idxs_triplet_not_added += [row.name]
                    for prefix in ['head', 'tail']:
                        if not eval(f"{prefix}_ids"):
                            names_not_in_lut += [{
                                'entity_type': eval(f"{prefix}_type"),
                                'entity_name': row[prefix]
                            }]
                    return
                triplets_new_rows_, mdata_new_row_ \
                    = self._get_updated_triplets(head_ids, 'r1', tail_ids, row)
                triplets_new_rows += triplets_new_rows_
                mdata_new_rows += [mdata_new_row_]
            else:
                raise NotImplementedError

        # Keep track on the triplet adding outcome.
        idxs_triplet_not_added = []
        names_not_in_lut = []
        triplets_new_rows = []
        mdata_new_rows = []
        triplets.apply(_add_triplets, axis=1)

        def _expand_mdata(group):
            if group['_rid'].iloc[0] == 'r1':
                self._mdata_contains = pd.concat(
                    [
                        self._mdata_contains.astype('object'),
                        group.drop(columns=['_rid']).astype('object'),
                    ],
                    ignore_index=True,
                )
            else:
                raise NotImplementedError

        # Update the knowledge graph using the "hit" (found in lut) triplets.
        if triplets_new_rows:
            triplets_new = pd.DataFrame(triplets_new_rows)
            mdata_new = pd.DataFrame(mdata_new_rows)
            self._triplets = pd.concat(
                [self._triplets, triplets_new],
                ignore_index=True,
            )
            mdata_new.groupby('_rid').apply(_expand_mdata)

            self._triplets.to_csv(
                f"{self.path_kg}/{self._timestamp}/triplets.tsv",
                sep='\t',
                index=False,
            )
            self._mdata_contains.to_csv(
                f"{self.path_kg}/{self._timestamp}/mdata_contains.tsv",
                sep='\t',
                index=False,
            )
            print(
                f"{len(triplets_new)} triplets added by hitting the lookup tables. "
                f"{len(mdata_new)} metadata entries added."
            )

        # Deal with the "miss" triplets.
        if idxs_triplet_not_added:
            triplets_not_added = triplets.loc[idxs_triplet_not_added]
            names_not_in_lut = pd.DataFrame(names_not_in_lut).drop_duplicates()

            triplets_not_added.to_csv(
                f"{self.path_kg}/{self._timestamp}/triplets_not_added.tsv",
                sep='\t',
                index=False,
            )
            names_not_in_lut.groupby('entity_type').apply(
                lambda group: group.to_csv(
                    f"{self.path_kg}/{self._timestamp}/"
                    f"{group['entity_type'].iloc[0]}_names_not_in_lut.tsv",
                    sep='\t',
                    index=False,
                )
            )
            print(
                f"{len(triplets_not_added)} triplets not added due to missing "
                f"entities in the lookup tables. The file stored at {self.path_kg}/"
                f"{self._timestamp}. Please retrieve the corresponding primary IDs to "
                "proceed."
            )

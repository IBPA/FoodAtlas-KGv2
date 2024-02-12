from itertools import product

import pandas as pd
from pympler import asizeof

from .utils import load_entities, load_lookup_tables


class KnowledgeGraph:

    def __init__(
        self,
        path_kg="outputs/kg",
    ):
        self.path_kg = path_kg
        self._load_kg()

    def _load_kg(self):
        # Load the knowledge graph data.
        self._entities = load_entities(f"{self.path_kg}/entities.tsv")
        self._relations = pd.read_csv(f"{self.path_kg}/relationships.tsv", sep='\t')
        self._triplets = pd.read_csv(f"{self.path_kg}/triplets.tsv", sep='\t')
        self._mdata_contains \
            = pd.read_csv(f"{self.path_kg}/mdata_contains.tsv", sep='\t')

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

    def _update_triplets(
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
        # self._triplets = pd.concat([
        #     self._triplets, pd.DataFrame(triplets_new_rows)
        # ], ignore_index=True)
        # self._mdata_contains = pd.concat([
        #     self._mdata_contains, pd.DataFrame([mdata_new_row])
        # ], ignore_index=True)

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
        for col in ['head', 'relationship', 'tail']:
            if col not in triplets.columns:
                raise ValueError(f"Column '{col}' not found in triplets")

        def _add_triplet(row):
            nonlocal triplets_not_added, triplets_new_rows, mdata_new_rows

            if row['relationship'] == 'contains':
                head_ids = self._link_name_to_entities(row['head'], 'food')
                tail_ids = self._link_name_to_entities(row['tail'], 'chemical')
                if not head_ids or not tail_ids:
                    triplets_not_added += [row.name]
                    return
                triplets_new_rows_, mdata_new_row_ \
                    = self._update_triplets(head_ids, 'r1', tail_ids, row)
                triplets_new_rows += triplets_new_rows_
                mdata_new_rows += [mdata_new_row_]
            else:
                raise NotImplementedError

        triplets_not_added = []
        triplets_new_rows = []
        mdata_new_rows = []
        triplets.apply(_add_triplet, axis=1)
        triplets_new = pd.DataFrame(triplets_new_rows)
        mdata_new = pd.DataFrame(mdata_new_rows)

        print(triplets_new)
        print(mdata_new)
        # print(triplets_not_added)

        # self._triplets.to_csv(
        #     f"{self.path_kg}/triplets.tsv", sep='\t', index=False)
        # self._mdata_contains.to_csv(
        #     f"{self.path_kg}/mdata_contains.tsv", sep='\t', index=False)

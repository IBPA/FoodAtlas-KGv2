from ast import literal_eval

import pandas as pd

from .utils import load_entities, load_lookup_tables


class KnowledgeGraph:

    def __init__(
        self,
        path_kg="outputs/kg",
    ):
        self.path_kg = path_kg
        self._load_kg()

    def _load_kg(self):
        self._entities = load_entities(f"{self.path_kg}/entities.tsv")
        self._relations = pd.read_csv(f"{self.path_kg}/relationships.tsv", sep='\t')
        self._lut_food, self._lut_chemical = load_lookup_tables()

    def get_x(self):
        pass

    def _merge_entity(self, entity_new, entity_id):
        pass

    def _create_entity(self, entity_new):
        pass

    def _update_lookup_table(self):
        pass

    def _update_entities(self):
        pass

    def add_triplet(self, head_id, tail_id, relation_id, evidence):
        if relation_id == 'r1':
            pass
        elif relation_id == 'r2':
            pass

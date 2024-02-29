from ast import literal_eval

import pandas as pd

# from .utils.constants import constants


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


class LookupTables:
    """
    """

    def __init__(
        self,
        path_lut_food: str,
        path_lut_chemical: str,
    ):
        self.path_lut_food = path_lut_food
        self.path_lut_chemical = path_lut_chemical

        self._load()

    def _load(self):
        """
        """
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

    def _save(self, path_output_dir):
        """
        """
        pd.DataFrame(self._lut_food.items(), columns=['name', 'foodatlas_id']).to_csv(
            f"{path_output_dir}/lookup_table_food.tsv", sep='\t', index=False
        )
        pd.DataFrame(
            self._lut_chemical.items(), columns=['name', 'foodatlas_id']
        ).to_csv(
            f"{path_output_dir}/lookup_table_chemical.tsv", sep='\t', index=False
        )

    def create(self):
        pass

    def update(self):
        pass

    def get_entity_ids(
        self,
        entity_type: str,
        entity_name: str,
    ) -> list[str]:
        """
        """
        if entity_type == 'food':
            lut = self._lut_food
        elif entity_type == 'chemical':
            lut = self._lut_chemical
        else:
            raise ValueError(f'Invalid entity_type: {entity_type}')

        return lut[entity_name] if entity_name in lut else []

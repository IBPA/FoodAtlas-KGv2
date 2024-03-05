from pandarallel import pandarallel

from ..kg import KnowledgeGraph

pandarallel.initialize(progress_bar=True)


if __name__ == '__main__':
    kg = KnowledgeGraph()

    # Test 1. All synonyms in the entities can be found in LUT.
    def _check_entity_name_in_lut(row):
        for name in row['synonyms']:
            if row['entity_type'] == 'food':
                assert name in kg.entities._lut_food, \
                    f"Test 1 failed: food {name} {row['foodatlas_id']}"
            elif row['entity_type'] == 'chemical':
                assert name in kg.entities._lut_chemical, \
                    f"Test 1 failed: chemical {name} {row['foodatlas_id']}"
            else:
                raise NotImplementedError()

    kg.entities._entities.parallel_apply(_check_entity_name_in_lut, axis=1)

    # Test 2. All entities in the lookup table are in the entities.
    entities_in_lut = set()
    for entities in kg.entities._lut_food.values():
        entities_in_lut.update(entities)
    for entities in kg.entities._lut_chemical.values():
        entities_in_lut.update(entities)
    entities = set(kg.entities._entities.index.tolist())
    assert len(entities) == len(kg.entities._entities)
    assert entities_in_lut - entities == set(), "Test 2 failed."

    # Test 3. All triplets have valid entities.
    assert kg.triplets._triplets['foodatlas_id'].nunique() == len(kg.triplets._triplets)

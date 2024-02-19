from ..kg import KnowledgeGraph

# # Check all LUT entities are in the entities.
# pass

# # Check triplets from Lit2KG


if __name__ == '__main__':
    kg = KnowledgeGraph()

    # Test 1. All synonyms in the entities can be found in LUT.
    def _check_entity_name_in_lut(row):
        for name in row['synonyms']:
            if row['entity_type'] == 'food':
                assert name in kg._lut_food, "Test 1 failed."
            elif row['entity_type'] == 'chemical':
                assert name in kg._lut_chemical, "Test 1 failed."
            else:
                raise NotImplementedError()

    kg._entities.apply(_check_entity_name_in_lut, axis=1)

    # Test 2. All entities in the lookup table are in the entities.
    entities_in_lut = set()
    for entities in kg._lut_food.values():
        entities_in_lut.update(entities)
    for entities in kg._lut_chemical.values():
        entities_in_lut.update(entities)
    entities = set(kg._entities['foodatlas_id'])
    assert len(entities) == len(kg._entities)
    assert entities_in_lut - entities == set(), "Test 2 failed."

    # Test 3. All triplets have valid entities.
    assert kg._triplets['foodatlas_id'].nunique() == len(kg._triplets)

    # Test 4. Use food to retrieve existing triplets and metadata.
    food = "milk"
    mdata = kg.get_triplets_by_food_name(food)
    print(mdata)
    mdata.to_csv("milk.csv")

    # Test 5. All foods that contain "milk" in their names.
    foods = [x for x in list(kg._lut_food.keys()) if 'milk' in x]
    print(foods)
    for food in foods:
        mdata = kg.get_triplets_by_food_name(food)
        print(mdata)

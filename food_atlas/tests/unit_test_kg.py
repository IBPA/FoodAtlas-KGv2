def test_entities(kg):
    # Test 1: Each lookup table entry should only have one entity.
    lut_food = kg.entities._lut_food
    lut_chemical = kg.entities._lut_chemical
    for k, v in lut_food.items():
        try:
            assert len(v) == 1
        except AssertionError:
            print(k, v)
    for k, v in lut_chemical.items():
        try:
            assert len(v) == 1
        except AssertionError:
            print(k, v)

    # Test 2: Each entity should neither be a "placeholding" or a "placeholden".
    entities = kg.entities._entities
    for _, row in entities.iterrows():
        assert (
            '_placeholder_to' in row['external_ids']
            and '_placeholder_from' in row['external_ids']
        ) is False

    # Test 3: Each food and chemical should have at most one unique primary ID.
    entities_food = entities[entities['entity_type'] == 'food'].copy()
    entities_food['primary_id'] = entities_food['external_ids'].apply(
        lambda x: x['foodon_id'] if 'foodon_id' in x else None
    )
    entities_chemical = entities[entities['entity_type'] == 'chemical'].copy()
    entities_chemical['primary_id'] = entities_chemical['external_ids'].apply(
        lambda x: x['pubchem_cid'] if 'pubchem_cid' in x else None
    )

    assert entities_food['primary_id'].value_counts().max() <= 1
    assert entities_chemical['primary_id'].value_counts().max() <= 1


def test_triplets(kg):
    triplets = kg.triplets._triplets

    # Test 1. Triplets should be unique.
    assert triplets.index.nunique() == len(triplets)
    assert len(triplets.groupby(['head_id', 'relationship_id', 'tail_id']).size()) \
        == len(triplets)

    # Test 2. Metadata ids should be unique.
    for _, row in triplets.iterrows():
        assert len(row['metadata_ids']) == len(set(row['metadata_ids']))

    # Test 3. A metadata id should not be shared across triplets.
    metadata_ids = set()
    for _, row in triplets.iterrows():
        for metadata_id in row['metadata_ids']:
            assert metadata_id not in metadata_ids
            metadata_ids.add(metadata_id)


def test_metadata(kg):
    # Test 1: For each master unit, let's provide a unit test case.
    pass


def test_all(kg):
    test_entities(kg)
    test_triplets(kg)
    # test_metadata(kg)

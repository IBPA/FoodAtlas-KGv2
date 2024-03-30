def test_triplets(kg):
    triplets = kg.triplets._triplets

    # Test 1. Triplets should be unique.
    assert triplets['foodatlas_id'].nunique() == len(triplets)
    assert len(triplets.groupby(['head_id', 'relationship_id', 'tail_id']).size()) \
        == len(triplets)

    # Test 2. Metadata ids should be unique.
    for _, row in triplets.iterrows():
        assert len(row['metadata_ids']) == len(set(row['metadata_ids']))


def test_metadata(kg):
    # Test 1: For each master unit, let's provide a unit test case.
    pass


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


def test_all(kg):
    test_entities(kg)
    # test_triplets(kg)
    # test_metadata(kg)

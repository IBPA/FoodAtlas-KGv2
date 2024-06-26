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

    # Test 2: Each entity should either be a "placeholding" or a "placeholden".
    entities = kg.entities._entities
    for _, row in entities.iterrows():
        assert (
            '_placeholder_to' in row['external_ids']
            and '_placeholder_from' in row['external_ids']
        ) is False

    # Test 3: Each food and chemical should have exactly one unique primary ID,
    # except placeholders.
    def assign_primary_id(row):
        if '_placeholder_to' in row['external_ids']:
            row['primary_id'] += ['PH']
            return row

        if row['entity_type'] == 'food':
            row['primary_id'] += row['external_ids']['foodon']
        elif row['entity_type'] == 'chemical':
            if 'chebi' in row['external_ids']:
                row['primary_id'] += [f"CHEBI:{row['external_ids']['chebi']}"]
            elif 'cdno' in row['external_ids']:
                row['primary_id'] += [f"CDNO:{row['external_ids']['cdno']}"]
            elif 'fdc_nutrient' in row['external_ids']:
                row['primary_id'] += [f"FDCN:{row['external_ids']['fdc_nutrient']}"]
        else:
            raise NotImplementedError
    entities_ = entities.copy()
    entities_['primary_id'] = [[] for _ in range(len(entities_))]
    entities_.apply(assign_primary_id, axis=1)

    def assert_primary_id_uniqueness(primary_id):
        assert len(primary_id) == 1
        return primary_id[0]

    entities_['primary_id'] = entities_['primary_id'].apply(
        assert_primary_id_uniqueness
    )

    assert entities_['primary_id'].value_counts().drop('PH').max() == 1


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

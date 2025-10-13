from ast import literal_eval

import pandas as pd
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=True)


def load_entities(with_connections=True):
    """Load entities.tsv.

    Args:
        with_connections (bool): If True, load entities with at least one connection to
            a food entity.

    """
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
        converters={
            'external_ids': literal_eval,
        },
    ).set_index('foodatlas_id')

    if with_connections:
        entities['count'] = 0
        triplets = pd.read_csv(
            "outputs/kg/triplets.tsv",
            sep='\t',
            converters={
                'metadata_ids': literal_eval,
            }
        )
        triplets_r1 = triplets[triplets['relationship_id'] == 'r1']
        for _, row in triplets_r1.iterrows():
            head_id = row['head_id']
            tail_id = row['tail_id']
            entities.at[head_id, 'count'] += len(row['metadata_ids'])
            entities.at[tail_id, 'count'] += len(row['metadata_ids'])
        entities = entities[entities['count'] > 0].copy()

    return entities


def print_stats_with_connections():
    """Print various statistics for entities with connections.

    """
    entities = load_entities(True)
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep='\t',
        converters={
            'metadata_ids': literal_eval,
        }
    )
    food_ontologies = pd.read_csv(
        "outputs/kg/food_ontology.tsv",
        sep='\t',
    )
    chemical_ontologies = pd.read_csv(
        "outputs/kg/chemical_ontology.tsv",
        sep='\t',
    )
    entities.query("entity_type == 'food'").sort_values(
        'count',
        ascending=False,
    ).to_csv(
        'foods_with_evidence_count.tsv',
        sep='\t',
    )

    foods_with_connections = entities.query("entity_type == 'food' and count > 0")

    print("Foods with connections:")
    print(f"Entities: {len(foods_with_connections)}")
    print()

    chemicals = set(entities[entities['entity_type'] == 'chemical'].index.tolist())

    print("Chemicals with connections:")
    print(f"Entities: {len(chemicals)}")
    print()

    food_chemical_triplets = triplets.query("relationship_id == 'r1'")

    print("Food-chemical associations:")
    print(f"Triplets: {len(food_chemical_triplets)}")
    print()

    entities_disease = set()
    triplets_pos_disease = set()
    metadata_pos_disease = set()
    triplets_neg_disease = set()
    metadata_neg_disease = set()
    for _, row in triplets.query("relationship_id in ['r3', 'r4']").iterrows():
        if row['head_id'] not in chemicals:
            continue
        entities_disease.add(row['tail_id'])

        if row['relationship_id'] == 'r3':
            triplets_pos_disease.add(row['foodatlas_id'])
            metadata_pos_disease.update(row['metadata_ids'])
        else:
            triplets_neg_disease.add(row['foodatlas_id'])
            metadata_neg_disease.update(row['metadata_ids'])

    print("Disease with connections:")
    print(f"Entities: {len(entities_disease)}")
    print(f"Triplets (Pos): {len(triplets_pos_disease)}")
    print(f"Metadata (Pos): {len(metadata_pos_disease)}")
    print(f"Triplets (Neg): {len(triplets_neg_disease)}")
    print(f"Metadata (Neg): {len(metadata_neg_disease)}")
    print()

    entities_flavor = set()
    triplets_flavor = set()
    metadata_flavor = set()
    for _, row in triplets.query("relationship_id == 'r5'").iterrows():
        if row['head_id'] not in chemicals:
            continue
        entities_flavor.add(row['tail_id'])
        triplets_flavor.add(row['foodatlas_id'])
        metadata_flavor.update(row['metadata_ids'])

    print("Flavor with connections:")
    print(f"Entities: {len(entities_flavor)}")
    print(f"Triplets: {len(triplets_flavor)}")
    print(f"Metadata: {len(metadata_flavor)}")

    # All triplets with connections
    triplets_r1 = triplets[(triplets['relationship_id'] == 'r1') & (triplets['head_id'].isin(foods_with_connections.index))]

    def dfs(onto, entities):
        visited = set()
        is_a_map = {row['head_id']: row['tail_id'] for _, row in onto.iterrows()}
        for entity in entities:
            curr = entity
            while curr in is_a_map and curr not in visited:
                visited.add(curr)
                curr = is_a_map[curr]
        return len(visited)
    n_triplets_r2 = dfs(food_ontologies, foods_with_connections.index) + dfs(chemical_ontologies, list(chemicals))
    triplets_r3 = triplets[(triplets['relationship_id'] == 'r3') & (triplets['head_id'].isin(chemicals))]
    triplets_r4 = triplets[(triplets['relationship_id'] == 'r4') & (triplets['head_id'].isin(chemicals))]
    triplets_r5 = triplets[(triplets['relationship_id'] == 'r5') & (triplets['head_id'].isin(chemicals))]
    print(f"Triplets (r1): {len(triplets_r1)}")
    print(f"Triplets (r2): {n_triplets_r2}")
    print(f"Triplets (r3): {len(triplets_r3)}")
    print(f"Triplets (r4): {len(triplets_r4)}")
    print(f"Triplets (r5): {len(triplets_r5)}")

    print(f"Total: {len(triplets_r1) + n_triplets_r2 + len(triplets_r3) + len(triplets_r4) + len(triplets_r5)}")


def print_entities_db_coverage():
    entities = load_entities(False)
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep='\t',
        converters={
            'external_ids': literal_eval,
        }
    )
    print(entities)

    # Food entities.
    triplets_r1 = triplets[triplets['relationship_id'] == 'r1']
    food_entities = entities.loc[triplets_r1['head_id'].unique()]
    food_ids = food_entities['external_ids'].parallel_apply(pd.Series)
    print(len(food_ids))
    print(food_ids.notnull().sum(), food_ids.notnull().sum() / len(food_ids))

    # Chemical entities.
    chemical_entities = entities.loc[triplets_r1['tail_id'].unique()]
    chemical_ids = chemical_entities['external_ids'].parallel_apply(pd.Series)
    print(len(chemical_ids))
    print(chemical_ids.notnull().sum(), chemical_ids.notnull().sum() / len(chemical_ids))

    # Disease entities.
    triplets_r3 = triplets[triplets['relationship_id'] == 'r3']
    triplets_r4 = triplets[triplets['relationship_id'] == 'r4']
    disease_ids = set(triplets_r3['tail_id'].unique()) & set(triplets_r4['tail_id'].unique())
    disease_entities = entities.loc[list(disease_ids)]
    disease_ids = disease_entities['external_ids'].parallel_apply(pd.Series)
    print(len(disease_ids))
    print(disease_ids.notnull().sum(), disease_ids.notnull().sum() / len(disease_ids))

    # Flavor entities.
    triplets_r5 = triplets[(triplets['relationship_id'] == 'r5') & (triplets['head_id'].isin(chemical_entities.index))]
    flavordb_ids = list(set(triplets_r5['tail_id'].unique()))
    flavor_entities = entities.loc[entities.index.isin(flavordb_ids)]
    flavor_ids = flavor_entities['external_ids'].parallel_apply(pd.Series)
    print(flavor_ids)
    print(len(flavor_ids))
    print(flavor_ids.notnull().sum(), flavor_ids.notnull().sum() / len(flavor_ids))


if __name__ == '__main__':
    # print_stats_with_connections()
    print_entities_db_coverage()
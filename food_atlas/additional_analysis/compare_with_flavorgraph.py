from ast import literal_eval

import pandas as pd
from thefuzz import fuzz


def load_flavorgraph():
    nodes = pd.read_csv("data/FlavorGraph/nodes_191120.csv").set_index('node_id')
    edges = pd.read_csv("data/FlavorGraph/edges_191120.csv")
    edges = edges[edges['edge_type'] != 'ingr-ingr']

    foods = nodes.loc[edges['id_1'].unique()]
    assert (foods['node_type'] == 'ingredient').all()
    chemicals = nodes.loc[edges['id_2'].unique()]
    assert (chemicals['node_type'] == 'compound').all()

    foods['name'] = foods['name'].str.replace('_', ' ').str.lower()

    print(len(foods), len(chemicals))

    return foods, chemicals


def fuzzy_match(food_name, lut_foods):
    candidates = []
    for lut_food_name in lut_foods.index:
        if fuzz.token_set_ratio(food_name, lut_food_name) > 90:
            print(f"Fuzzy match found {food_name} -> {lut_food_name}")
            candidates.append((
                lut_food_name, lut_foods.loc[lut_food_name]['foodatlas_id'][0]
            ))
    return candidates



if __name__ == '__main__':
    fg_foods, fg_chemicals = load_flavorgraph()

    entities = pd.read_csv("outputs/kg/entities.tsv", sep='\t').set_index('foodatlas_id')
    triplets = pd.read_csv("outputs/kg/triplets.tsv", sep='\t')
    triplets_r1 = triplets[triplets['relationship_id'] == 'r1']
    foods = entities[entities.index.isin(triplets_r1['head_id'].unique())]
    chemicals = entities[entities.index.isin(triplets_r1['tail_id'].unique())].copy()
    triplets_flavor = triplets[triplets['relationship_id'] == 'r5']
    triplets_flavor = triplets_flavor[triplets_flavor['head_id'].isin(chemicals.index)]
    flavor_chemicals = chemicals[chemicals.index.isin(triplets_flavor['head_id'].unique())]
    flavor_cids = flavor_chemicals['external_ids'].apply(lambda x: literal_eval(x)['pubchem_compound'][0]).unique().tolist()

    # Food.
    lut_foods = pd.read_csv(
        "outputs/kg/lookup_table_food.tsv",
        sep='\t',
        converters={'foodatlas_id': literal_eval},
    ).set_index('name')

    n_found = 0
    fg_foods['matched_eids'] = [[] for _ in fg_foods.index]
    for i, row in fg_foods.iterrows():
        hit = False
        food_name = row['name']

        # Check exact match.
        if food_name in lut_foods.index:
            if lut_foods.loc[food_name]['foodatlas_id'][0] in foods.index:
                fg_foods.loc[i]['matched_eids'].append(lut_foods.loc[food_name]['foodatlas_id'][0])
                n_found += 1
                hit = True
        else:
            candidates = fuzzy_match(food_name, lut_foods)
            if candidates:
                for hit_name, hit_eid in candidates:
                    if hit_eid in foods.index:
                        fg_foods.loc[i]['matched_eids'].append(hit_eid)
                        n_found += 1
                        hit = True

        if not hit:
            print(f"Not found {row['name']} in FoodAtlas")

    print(f"# FoodAtlas: {len(foods)}")
    print(f"# FlavorGraph: {len(fg_foods)}")
    print(f"# food name hits: {n_found} ({n_found / len(fg_foods):.4f})")

    # Chemicals.
    fg_chemicals = fg_chemicals.copy()
    fg_chemicals['matched_eids'] = [[] for _ in fg_chemicals.index]
    chemicals['external_ids'] = chemicals['external_ids'].apply(literal_eval)
    cids_to_eids = {}
    for i, row in chemicals.iterrows():
        for cid in row['external_ids'].get('pubchem_compound', []):
            if cid not in cids_to_eids:
                cids_to_eids[cid] = []
            cids_to_eids[cid].append(row.name)

    n_found = 0
    for i, row in fg_chemicals.iterrows():
        id_ = row['id']
        if id_ in cids_to_eids:
            n_found += 1
            fg_chemicals.loc[i]['matched_eids'].extend(cids_to_eids[id_])

    print(f"# FoodAtlas: {len(chemicals)}")
    print(f"# FlavorGraph: {len(fg_chemicals)}")
    print(f"# Chemical id hits: {n_found} ({n_found / len(fg_chemicals):.4f})")

    # # Flavor
    # fg_flavor_chemicals = fg_chemicals.query("is_hub == 'food'")
    # n_found = 0
    # for i, row in fg_flavor_chemicals.iterrows():
    #     id_ = row['id']
    #     if id_ in set(flavor_cids):
    #         n_found += 1
    #         fg_flavor_chemicals.loc[i]['matched_eids'] = cids_to_eids[id_]

    # print(f"# FoodAtlas Flavor Chemicals: {len(flavor_cids)}")
    # print(f"# FlavorGraph Flavor Chemicals: {len(fg_flavor_chemicals)}")
    # print(f"# Flavor Chemical id hits: {n_found} ({n_found / len(fg_flavor_chemicals):.4f})")

    # Triplets.
    fg_edges = pd.read_csv("data/FlavorGraph/edges_191120.csv")
    fg_edges = fg_edges[fg_edges['edge_type'] != 'ingr-ingr'].copy()

    def match_edge(row):
        food_matched = fg_foods.loc[row['id_1']]['matched_eids']
        chemical_matched = fg_chemicals.loc[row['id_2']]['matched_eids']
        if len(food_matched) > 0 and len(chemical_matched) > 0:
            return True
        return False

    fg_edges['is_in_fa'] = fg_edges.apply(match_edge, axis=1)
    print(f"# FoodAtlas: {len(triplets_r1)}")
    print(f"# FlavorGraph edges: {len(fg_edges)}")
    print(f"# FlavorGraph edges in FoodAtlas: {fg_edges['is_in_fa'].sum()}")

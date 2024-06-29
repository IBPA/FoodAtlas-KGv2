import pandas as pd
from tqdm import tqdm

from .. import KnowledgeGraph

tqdm.pandas()


def generate_food_groups(kg):
    entities = kg.entities._entities.query("entity_type == 'food'").copy()
    triplets = kg.triplets._triplets
    foodonto = pd.read_csv("outputs/kg/food_ontology.tsv", sep='\t')\
        .query("source == 'foodon'")

    ht_is_a = {}
    for _, row in foodonto.iterrows():
        if row['head_id'] not in ht_is_a:
            ht_is_a[row['head_id']] = []
        ht_is_a[row['head_id']] += [row['tail_id']]

    def count_metadata(eid):
        count = 0
        triplets_ = triplets.query(f"head_id == '{eid}'")
        for _, row in triplets_.iterrows():
            count += len(row['metadata_ids'])
        return count

    entities['metadata_count'] = entities.apply(
        lambda row: count_metadata(row.name),
        axis=1,
    )

    # Food groups.
    map_food_groups = {
        'dairy food product': 'dairy',
        'plant fruit food product': 'fruit',
        'plant seed or nut food product': 'plant seed or nut',
        'legume food product': 'legume',
        'vegetable food product': 'vegetable',
        'plant food product': 'other plant',
        'mammalian meat food product': 'mammalian meat',
        'avian food product': 'avian',
        'animal seafood product': 'seafood',
        'fish food product': 'seafood',
        'animal food product': 'other animal',
        # 'animal food product': 'animal food product',
        # 'plant food product': 'plant food product',
        # 'algal food product',
        # 'fungus food product',
        # 'microbial food product',
    }
    eids_food_group = [kg.entities._lut_food[x][0] for x in map_food_groups.keys()]

    ht = {}
    for eid, food_group_name in zip(eids_food_group, map_food_groups.values()):
        ht[eid] = [food_group_name]

    def traverse():
        def dfs(eid):
            if eid in ht:
                return ht[eid]
            res = []
            parents = ht_is_a.get(eid, [])
            for parent in parents:
                res += dfs(parent)
            res = list(set(res))
            ht[eid] = res

            return res

        entities.apply(lambda row: dfs(row.name), axis=1)

    traverse()

    def clean_groups(groups):
        if 'other plant' in groups \
                and (
                    'fruit' in groups
                    or 'legume' in groups
                    or 'plant seed or nut' in groups
                    or 'vegetable' in groups
                ):
            groups.remove('other plant')

        if 'other animal' in groups \
                and (
                    'seafood' in groups
                    or 'avian' in groups
                    or 'mammalian meat' in groups
                    or 'dairy' in groups
                ):
            groups.remove('other animal')

        if 'vegetable' in groups \
                and (
                    'fruit' in groups
                    or 'legume' in groups
                    or 'plant seed or nut' in groups
                ):
            groups.remove('vegetable')

        if 'legume' in groups \
                and 'plant seed or nut' in groups:
            groups.remove('plant seed or nut')

        if 'mammalian meat' in groups \
                and (
                    'avian' in groups
                    or 'seafood' in groups
                ):
            groups.remove('mammalian meat')

        if len(groups) != 1:
            return 'unclassified'
        else:
            return groups[0]

    entities['group'] = entities.index.map(ht)
    entities['group'] = entities['group'].apply(clean_groups)
    entities['group'].to_csv("outputs/kg/food_groups.tsv", sep='\t')


def generate_chemical_groups(kg):
    entities = kg.entities._entities.query("entity_type == 'chemical'").copy()
    chemonto = pd.read_csv("outputs/kg/chemical_ontology.tsv", sep='\t')\
        .query("source == 'chebi'")

    ht_is_a = {}
    for _, row in chemonto.iterrows():
        if row['head_id'] not in ht_is_a:
            ht_is_a[row['head_id']] = []
        ht_is_a[row['head_id']] += [row['tail_id']]
    # eids_food_group = [kg.entities._lut_food[x][0] for x in map_food_groups.keys()]

    # ht = {}
    # for eid, food_group_name in zip(eids_food_group, map_food_groups.values()):
    #     ht[eid] = [food_group_name]

    ht_has_child = {}
    for k, v in ht_is_a.items():
        for parent in v:
            if parent not in ht_has_child:
                ht_has_child[parent] = set()
            ht_has_child[parent].add(k)

    # Get groups.
    def get_group_eids(level=1):
        queue = ['e13287'] # level = 0
        while queue and level > 0:
            groups_new = []
            for current in queue:
                if current in ht_has_child:
                    groups_new.extend(ht_has_child[current])
                else:
                    groups_new.append(current)

            queue = list(set(groups_new))
            level -= 1

        return queue

    group_eids = get_group_eids()

    def map_group(row):
        if 'chebi' not in row['external_ids']:
            row['chebi'] = ['Unclassified']
            return row

        groups_matched = []
        queue = [row.name]
        while queue:
            current = queue.pop(0)
            if current not in ht_is_a:
                continue
            if current in group_eids:
                groups_matched.append(current)
            else:
                queue.extend(ht_is_a[current])

        row['chebi'] = list(set(sorted(
            entities.loc[groups_matched]['common_name'].str.capitalize().tolist()
        )))

        return row

    entities['chebi'] = None
    entities = entities.progress_apply(map_group, axis=1)
    chemical_groups = entities[['chebi']]
    chemical_groups.to_csv("outputs/kg/chemical_groups.tsv", sep='\t')


if __name__ == '__main__':
    kg = KnowledgeGraph()

    generate_food_groups(kg)
    generate_chemical_groups(kg)

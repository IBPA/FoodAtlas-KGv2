import pandas as pd
from tqdm import tqdm

tqdm.pandas()


def generate_chemical_groups_chebi(kg):
    """
    """
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
            row['chebi'] = ['unclassified']
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
            entities.loc[groups_matched]['common_name'].tolist()
        )))

        return row

    entities['chebi'] = None
    entities = entities.progress_apply(map_group, axis=1)
    chemical_groups = entities[['chebi']]

    return chemical_groups

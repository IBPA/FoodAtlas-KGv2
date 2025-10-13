import pandas as pd
from tqdm import tqdm

tqdm.pandas()


def generate_food_groups_foodon(kg, level=1):
    entities = kg.entities._entities.query("entity_type == 'food'").copy()
    triplets = kg.triplets._triplets
    foodonto = pd.read_csv("outputs/kg/food_ontology.tsv", sep="\t").query(
        "source == 'foodon'"
    )

    ht_is_a = {}
    for _, row in foodonto.iterrows():
        if row["head_id"] not in ht_is_a:
            ht_is_a[row["head_id"]] = []
        ht_is_a[row["head_id"]] += [row["tail_id"]]

    ht_has_child = {}
    for k, v in ht_is_a.items():
        for parent in v:
            if parent not in ht_has_child:
                ht_has_child[parent] = set()
            ht_has_child[parent].add(k)

    def count_metadata(eid):
        count = 0
        triplets_ = triplets.query(f"head_id == '{eid}'")
        for _, row in triplets_.iterrows():
            count += len(row["metadata_ids"])
        return count

    entities["metadata_count"] = entities.apply(
        lambda row: count_metadata(row.name),
        axis=1,
    )

    # Food groups.
    map_food_groups = {
        "dairy food product": "dairy",
        "plant fruit food product": "fruit",
        "plant seed or nut food product": "plant seed or nut",
        "legume food product": "legume",
        "vegetable food product": "vegetable",
        "plant food product": "other plant",
        "mammalian meat food product": "mammalian meat",
        "avian food product": "avian",
        "animal seafood product": "seafood",
        "fish food product": "seafood",
        "animal food product": "other animal",
    }
    eids_food_group = [kg.entities._lut_food[x][0] for x in map_food_groups.keys()]

    ht = {}
    if level == 1:
        for eid, food_group_name in zip(
            eids_food_group, map_food_groups.values(),
            strict=False,
        ):
            ht[eid] = [food_group_name]
    elif level == 2:
        for eid, _ in zip(
            eids_food_group, map_food_groups.values(),
            strict=False,
        ):
            children = list(ht_has_child.get(eid, set()))
            for child in children:
                if child not in ht:
                    ht[child] = []
                ht[child].append(entities.loc[child]["common_name"])
        ht = {k: list(set(v)) for k, v in ht.items()}
    else:
        raise ValueError(f"Invalid level: {level}")

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
        if "other plant" in groups and (
            "fruit" in groups
            or "legume" in groups
            or "plant seed or nut" in groups
            or "vegetable" in groups
        ):
            groups.remove("other plant")

        if "other animal" in groups and (
            "seafood" in groups
            or "avian" in groups
            or "mammalian meat" in groups
            or "dairy" in groups
        ):
            groups.remove("other animal")

        if "vegetable" in groups and (
            "fruit" in groups or "legume" in groups or "plant seed or nut" in groups
        ):
            groups.remove("vegetable")

        if "legume" in groups and "plant seed or nut" in groups:
            groups.remove("plant seed or nut")

        if "mammalian meat" in groups and ("avian" in groups or "seafood" in groups):
            groups.remove("mammalian meat")

        if len(groups) != 1:
            return ["unclassified"]
        else:
            return [groups[0]]

    entities["foodon"] = entities.index.map(ht)
    entities["foodon"] = entities["foodon"].apply(clean_groups)

    return entities["foodon"]

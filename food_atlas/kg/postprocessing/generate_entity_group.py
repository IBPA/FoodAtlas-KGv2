from ast import literal_eval

import pandas as pd
from tqdm import tqdm

from .. import KnowledgeGraph

tqdm.pandas()


def load_ht_is_a(kg):
    triplets = kg.triplets._triplets
    triplets = triplets.query("relationship_id == 'r2'")

    ht_is_a = {}
    for _, row in triplets.iterrows():
        if row['head_id'] not in ht_is_a:
            ht_is_a[row['head_id']] = []
        ht_is_a[row['head_id']] += [row['tail_id']]

    return ht_is_a


def generate_food_groups(kg):
    entities = kg.entities._entities.query("entity_type == 'food'").copy()
    triplets = kg.triplets._triplets.query("relationship_id == 'r1'")
    ht_is_a = load_ht_is_a(kg)

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

    # entities.to_csv("check_food_groups.tsv", sep='\t')
    # print(entities['group'].value_counts())
    # print(entities.query("metadata_count != 0")['group'].value_counts())
    # entities.to_csv("check.tsv", sep='\t')

    # exit()

    # triplets = pd.read_csv(
    #     "outputs/kg/triplets.tsv",
    #     sep='\t',
    # )
    # triplets = triplets.query("relationship_id == 'r2'").copy()

    # print(triplets)
    # exit()

    # entities_food = entities[entities['entity_type'] == 'food'].copy()

    # print(entities_food)

    # exit()

    # entities_food['ncbi_taxonomy_division'] = None
    # records = pd.read_csv(
    #     "outputs/kg/_cache/_cached_fetch_ncbi_taxonomy.tsv",
    #     sep='\t',
    #     converters={
    #         'Division': str,
    #     },
    # )
    # ncbi_taxon_id2division = dict(zip(records['TaxId'], records['Division']))

    # entities_food['ncbi_taxonomy_division'] = entities_food['external_ids'].apply(
    #     lambda x: ncbi_taxon_id2division[x['ncbi_taxon_id']]
    #     if 'ncbi_taxon_id' in x else 'Unclassified'
    # )

    # entities_food['ncbi_taxonomy_division'].to_csv(
    #     "outputs/kg/food_groups.tsv",
    #     sep='\t',
    # )


def generate_chemical_groups():
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    ).set_index('foodatlas_id')
    entities_chemical = entities[entities['entity_type'] == 'chemical'].copy()
    entities_chemical['chebi_ids'] = entities_chemical['external_ids'].apply(
        lambda x: x['chebi_ids'] if 'chebi_ids' in x else []
    )

    chebi_chemicals = pd.read_csv(
        "data/ChEBI/compounds.tsv",
        sep='\t',
        encoding='unicode_escape',
    ).set_index('ID')
    chebi_relationships = pd.read_csv(
        "data/ChEBI/relation.tsv",
        sep='\t',
        encoding='unicode_escape',
    ).set_index(['FINAL_ID', 'TYPE', 'INIT_ID'])

    def get_parents(chebi_ids):
        def get_parent(chebi_id):
            if chebi_id not in chebi_relationships.index:
                return

            parents = chebi_relationships.loc[(chebi_id, 'is_a', slice(None))]

            if chebi_id not in ht_is_a:
                ht_is_a[chebi_id] = set()
            ht_is_a[chebi_id].update(parents.index.tolist())

            for parent in parents.index.tolist():
                if parent not in ht_is_a:
                    get_parent(parent)

        for chebi_id in chebi_ids:
            get_parent(chebi_id)

    ht_is_a = {}
    ht_is_a[24431] = set()
    entities_chemical['chebi_ids'].progress_apply(get_parents)
    # chebi_chemicals.loc[ht_is_a.keys()].to_csv("chebi.tsv", sep='\t')

    ht_has_child = {}
    for k, v in ht_is_a.items():
        for parent in v:
            if parent not in ht_has_child:
                ht_has_child[parent] = set()
            ht_has_child[parent].add(k)

    # Get groups.
    def get_groups(level=2):
        queue = [24431] # level = 0
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

    groups = get_groups()
    chebi_groups = chebi_chemicals.loc[groups]

    def map_group(chebi_ids):
        if not chebi_ids:
            return 'Unclassified'

        groups_matched = []
        queue = chebi_ids
        while queue:
            current = queue.pop(0)
            if current not in ht_is_a:
                continue

            if current in groups:
                groups_matched.append(current)
            else:
                queue.extend(ht_is_a[current])

        return list(set(
            chebi_groups.loc[groups_matched]['NAME'].str.capitalize().tolist()
        ))

    entities_chemical['chebi_groups'] = entities_chemical['chebi_ids'].progress_apply(
        lambda x: map_group(x)
    )
    # entities_chemical.to_csv("entities_chemical_with_groups.csv")
    chemical_groups = entities_chemical[['chebi_groups']]
    chemical_groups.to_csv("outputs/kg/chemical_groups.tsv", sep='\t')


if __name__ == '__main__':
    kg = KnowledgeGraph()

    generate_food_groups(kg)

    # generate_chemical_groups()

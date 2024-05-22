from ast import literal_eval

import pandas as pd
from tqdm import tqdm

tqdm.pandas()


def generate_food_groups():
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
        converters={
            'synonyms': literal_eval,
            'external_ids': literal_eval,
        },
    ).set_index('foodatlas_id')

    # Food groups.
    entities_food = entities[entities['entity_type'] == 'food'].copy()
    entities_food['ncbi_taxonomy_division'] = None
    records = pd.read_csv(
        "outputs/kg/_cache/_cached_fetch_ncbi_taxonomy.tsv",
        sep='\t',
        converters={
            'Division': str,
        },
    )
    ncbi_taxon_id2division = dict(zip(records['TaxId'], records['Division']))

    entities_food['ncbi_taxonomy_division'] = entities_food['external_ids'].apply(
        lambda x: ncbi_taxon_id2division[x['ncbi_taxon_id']]
        if 'ncbi_taxon_id' in x else 'Unclassified'
    )

    entities_food['ncbi_taxonomy_division'].to_csv(
        "outputs/kg/food_groups.tsv",
        sep='\t',
    )


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
    # generate_food_groups()
    generate_chemical_groups()

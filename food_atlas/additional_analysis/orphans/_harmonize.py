import os
import json
from ast import literal_eval

import requests
import pandas as pd
from pandarallel import pandarallel

pandarallel.initialize(progress_bar=True)


def query_wikipedia():
    from tqdm import tqdm
    tqdm.pandas()

    def _query_wikipedia(row):
        url = (
            "http://en.wikipedia.org/w/api.php?action=query&redirects&format=json"
            f"&titles={row['wikipedia_query_term']}"
        )
        text = requests.get(url).text

        return text

    if os.path.exists("entities_food_with_wiki_faster.tsv"):
        entities_food = pd.read_csv(
            "entities_food_with_wiki_faster.tsv",
            sep='\t',
            converters={'synonyms': literal_eval},
        ).set_index('foodatlas_id')
    else:
        entities = pd.read_csv(
            "outputs/kg/entities.tsv",
            sep='\t',
        ).set_index('foodatlas_id')
        entities_food = entities[entities['entity_type'] == 'food'].copy()
        entities_food['wikipedia_query_term'] = entities_food.apply(
            lambda row: row['scientific_name']
                if pd.notna(row['scientific_name'])
                else row['common_name'],
            axis=1,
        )
        entities_food['wikipedia_response'] = '<!DOCTYPE html>'

    entities_error = entities_food[
        entities_food['wikipedia_response'].str.contains('<!DOCTYPE html>')
    ].copy()
    while not entities_error.empty:
        if len(entities_error) > 1000:
            entities_error['wikipedia_response'] \
                = entities_error.parallel_apply(_query_wikipedia, axis=1)
        else:
            entities_error['wikipedia_response'] \
                = entities_error.progress_apply(_query_wikipedia, axis=1)

        entities_food.loc[entities_error.index] = entities_error
        entities_error = entities_food[
            entities_food['wikipedia_response'].str.contains('<!DOCTYPE html>')
        ].copy()
        entities_food.to_csv("entities_food_with_wiki_faster.tsv", sep='\t')

    def _parse_wikipedia_response(row):
        res = json.loads(row['wikipedia_response'])['query']['pages']

        return list(res.values())[0]['title'] if '-1' not in res else ''

    entities_food['wikipedia_title'] = entities_food.apply(
        _parse_wikipedia_response,
        axis=1,
    )
    entities_food.to_csv("entities_food_with_wiki_faster.tsv", sep='\t')


if __name__ == '__main__':
    query_wikipedia()

    # Get the number of entities that appeared in metadata.
    metadata = pd.read_csv(
        "outputs/kg/metadata_contains.tsv",
        sep='\t',
    ).set_index('foodatlas_id')
    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep='\t',
        converters={'metadata_ids': literal_eval},
    )
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
    ).set_index('foodatlas_id')

    def add_n_metadata(row):
        n = len(row['metadata_ids'])
        entities.loc[row['head_id'], 'count_all'] += n
        entities.loc[row['tail_id'], 'count_all'] += n

        for mid in row['metadata_ids']:
            if metadata.loc[mid, 'source'] == 'lit2kg:gpt-4':
                entities.loc[row['head_id'], 'count_gpt'] += 1
                entities.loc[row['tail_id'], 'count_gpt'] += 1

    entities['count_all'] = 0
    entities['count_gpt'] = 0
    triplets.apply(
        add_n_metadata,
        axis=1,
    )
    entities.to_csv("entities_count.tsv", sep='\t')
    print(entities)

    exit()
    # entities_food = pd.read_csv(
    #     "entities_food_with_wiki_faster.tsv",
    #     sep='\t',
    #     converters={'synonyms': literal_eval},
    # ).set_index('foodatlas_id')

    # Start from here.

    # # Q1: Are wikipedia a good source of synonym resolution?
    # entities_food['wiki_in_synonyms'] \
    #     = entities_food.apply(
    #         lambda row: row['wikipedia_title'].lower() in row['synonyms']
    #             if pd.notna(row['wikipedia_title']) else False,
    #         axis=1,
    #     )
    # print(entities_food)
    # entities_food.to_csv('check_again.csv')

    # Q2: How many chemicals are orphaned?
    entities = pd.read_csv(
        "outputs/kg/entities.tsv",
        sep='\t',
    ).set_index('foodatlas_id')
    entities_chemical = entities[entities['entity_type'] == 'chemical'].copy()
    entities_chemical.to_csv(
        "entities_chemical.tsv",
        sep='\t',
    )

    triplets = pd.read_csv(
        "outputs/kg/triplets.tsv",
        sep='\t',
        converters={'metadata_ids': literal_eval}
    )
    triplets_food_orphaned = triplets[
        triplets['head_id'].isin(entities_food_orphaned.index)
    ]
    print(triplets)
    print(triplets_food_orphaned)

    mids = []
    triplets_food_orphaned['metadata_ids'].apply(lambda x: mids.extend(x))
    print(len(set(mids)))
    metadata = pd.read_csv("outputs/kg/metadata_contains.tsv", sep='\t')
    print(metadata)
    metadata = metadata[metadata['foodatlas_id'].isin(set(mids))]
    metadata.to_csv("metadata_contains_orphaned.tsv", sep='\t')
    # metadata = pd.read_csv("outputs/kg/metadata_contains.tsv", sep='\t')
    # print(entities_food_orphaned)

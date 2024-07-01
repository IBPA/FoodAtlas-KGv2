import csv
import numpy as np
import pandas as pd

from ..kg import KnowledgeGraph
from ..kg.preprocessing import standardize_chemical_name


def parse_triplets_from_response(
    row: pd.Series,
):
    triplets_str = row['response'].split('\n')
    for triplet_str in triplets_str:
        triplet = next(
            csv.reader(
                [triplet_str],
                delimiter=',',
                quotechar='"',
                skipinitialspace=True,
            )
        )
        if len(triplet) == 4 and triplet[0] and triplet[2]:
            row['tuples'] += [triplet]
        else:
            if triplet:
                row['tuples_not_parsed'] += [triplet]


def format_tuple(
    row: pd.Series
):
    row[['_food_name', '_food_part', '_chemical_name', '_conc']] \
        = [x.strip().lower() for x in row['tuples']]
    row['_chemical_name'] = standardize_chemical_name(row['_chemical_name'])
    row['source'] = 'lit2kg:gpt-4'
    row['reference'] = {
        'text': row['sentence'],
        'pmcid': row['pmcid'],
    }
    row['entity_linking_method'] = 'exact_name_matching'

    return row


kg = KnowledgeGraph()

data_raw = pd.read_pickle("data/Lit2KG/text_parser_predicted_2024_02_25.pkl")
data_raw['tuples'] = [[] for _ in data_raw.index]
data_raw['tuples_not_parsed'] = [[] for _ in data_raw.index]
data_raw.apply(parse_triplets_from_response, axis=1)
data_raw_with_not_parsed = data_raw[data_raw['tuples_not_parsed'].apply(
    lambda x: True if len(x) != 0 else False
)]
n_tuples = 0
for row in data_raw.itertuples():
    n_tuples += len(row.tuples)
print(f"# tuples extracted: {n_tuples}")
columns = [
    'conc_value',
    'conc_unit',
    'food_part',
    'food_processing',
    'source',
    'reference',
    'entity_linking_method',
    'quality_score',
    '_food_name',
    '_chemical_name',
    '_conc',
    '_food_part',
]
metadata = data_raw.explode('tuples').dropna(subset=['tuples'])
metadata[columns] = None
metadata = metadata.parallel_apply(format_tuple, axis=1)
metadata = metadata[columns]

n_keywords = metadata['_food_name'].nunique() + metadata['_chemical_name'].nunique()
print(f"# unique keywords: {n_keywords}")

lut_food = pd.read_csv("outputs/kg/lookup_table_food.tsv", sep='\t')
lut_food = dict(zip(lut_food['name'], lut_food['foodatlas_id']))
lut_chemical = pd.read_csv("outputs/kg/lookup_table_chemical.tsv", sep='\t')
lut_chemical = dict(zip(lut_chemical['name'], lut_chemical['foodatlas_id']))

n_found = 0
for keyword in metadata['_food_name'].unique():
    if keyword in lut_food:
        n_found += 1
for keyword in metadata['_chemical_name'].unique():
    if keyword in lut_chemical:
        n_found += 1
print(f"# keywords found in lookup tables: {n_found}")
# TODO: Improve.
# For now, just assign FoodOn IDs by exact matching for foods.
# metadata = metadata[metadata['_food_name'].apply(lambda x: x in lut_food)]
# metadata = metadata[metadata['_chemical_name'].apply(lambda x: x in lut_chemical)]

metadata = kg.metadata._metadata_contains
print(metadata)

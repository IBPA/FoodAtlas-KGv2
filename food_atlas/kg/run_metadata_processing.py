# -*- coding: utf-8 -*-
"""

A run script for knowledge graph expansion.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

TODO:
    * Move standardizing chemical names to another file.

"""
import csv

import pandas as pd
from pandarallel import pandarallel
import click

from .preprocessing import standardize_chemical_conc, standardize_chemical_name

pandarallel.initialize(progress_bar=True)


def standardize_food_part():
    pass


def parse_triplets_from_response(
    row: pd.Series,
    model_name: str,
):
    response = row['response']
    if pd.isna(response):
        return

    triplets_str = response.split('\n')
    for triplet_str in triplets_str:
        if model_name == 'gpt-3.5-ft':
            if not triplet_str:
                continue
            if triplet_str[0] == '(' and triplet_str[-1] == ')':
                triplet_str = triplet_str[1:-1]

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
    row: pd.Series,
    model_name: str,
):
    row[['_food_name', '_food_part', '_chemical_name', '_conc']] \
        = [x.strip().lower() for x in row['tuples']]
    row['_chemical_name'] = standardize_chemical_name(row['_chemical_name'])
    row['source'] = f"lit2kg:{model_name}"
    row['reference'] = {
        'text': row['sentence'],
        'pmcid': row['pmcid'],
    }
    row['entity_linking_method'] = 'exact_name_matching'

    return row

@click.command()
@click.argument(
    'path-input',
)
@click.argument(
    'path-output-dir',
)
@click.option(
    '--model-name',
    default='gpt-4',
    help='Model name used for parsing triplets.',
)
def main(
    path_input: str,
    path_output_dir: str,
    model_name: str,
):
    if model_name not in ['gpt-4', 'gpt-3.5-ft']:
        raise ValueError(f"Invalid model name: {model_name}")

    if model_name == 'gpt-4':
        data_raw = pd.read_pickle(path_input)
    elif model_name == 'gpt-3.5-ft':
        data_raw = pd.read_csv(path_input, sep='\t')
    print(f"# sentences: {len(data_raw)}")

    data_raw['tuples'] = [[] for _ in data_raw.index]
    data_raw['tuples_not_parsed'] = [[] for _ in data_raw.index]
    data_raw.apply(lambda row: parse_triplets_from_response(row, model_name), axis=1)
    data_raw_with_not_parsed = data_raw[data_raw['tuples_not_parsed'].apply(
        lambda x: True if len(x) != 0 else False
    )]
    data_raw_with_not_parsed.to_csv(
        f"{path_output_dir}/_tuples_not_parsed.tsv",
        sep='\t',
        index=False,
    )
    print(f"# sentences with not parsable tuples: {len(data_raw_with_not_parsed)}")

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
    metadata = metadata.parallel_apply(
        lambda row: format_tuple(row, model_name),
        axis=1,
    )
    metadata = metadata[columns]

    # Stndardize the chemical concentration.
    metadata = standardize_chemical_conc(metadata)

    # Exact matching for foods and chemicals.
    lut_food = pd.read_csv("outputs/kg/lookup_table_food.tsv", sep='\t')
    lut_food = dict(zip(lut_food['name'], lut_food['foodatlas_id']))
    lut_chemical = pd.read_csv("outputs/kg/lookup_table_chemical.tsv", sep='\t')
    lut_chemical = dict(zip(lut_chemical['name'], lut_chemical['foodatlas_id']))

    food_name_hits = metadata['_food_name'].apply(lambda x: x in lut_food)
    chemical_name_hits = metadata['_chemical_name'].apply(lambda x: x in lut_chemical)

    food_name_hit_rate = food_name_hits.sum() / len(metadata)
    chemical_name_hit_rate = chemical_name_hits.sum() / len(metadata)
    print(f"# food name hits: {food_name_hits.sum()} ({food_name_hit_rate:.4f})")
    print(f"# chemical name hits: {chemical_name_hits.sum()} ({chemical_name_hit_rate:.4f})")

    unique_food_names = metadata['_food_name'].unique()
    unique_chemical_names = metadata['_chemical_name'].unique()
    unique_food_name_hits = [x in lut_food for x in unique_food_names]
    unique_chemical_name_hits = [x in lut_chemical for x in unique_chemical_names]
    unique_food_name_hit_rate = sum(unique_food_name_hits) / len(unique_food_names)
    unique_chemical_name_hit_rate = sum(unique_chemical_name_hits) / len(unique_chemical_names)
    print(f"# unique food names: {len(unique_food_names)}")
    print(f"# unique chemical names: {len(unique_chemical_names)}")
    print(f"# unique food name hits: {sum(unique_food_name_hits)} ({unique_food_name_hit_rate:.4f})")
    print(f"# unique chemical name hits: {sum(unique_chemical_name_hits)} ({unique_chemical_name_hit_rate:.4f})")

    metadata = metadata[food_name_hits & chemical_name_hits]

    print(f"# metadata after filtering: {len(metadata)}")

    metadata.to_csv(
        f"{path_output_dir}/_metadata_new.tsv",
        sep='\t',
        index=False,
    )

    # exit()
    # # Get unique list of chemical mentions for the exchange service.
    # print("Generating chemical mentions...")
    # chemicals = metadata['_chemical_name'].unique()
    # chemicals = pd.DataFrame(chemicals)
    # chemicals.to_csv(
    #     f"{path_output_dir}/_chemical_mentions.tsv",
    #     sep='\t',
    #     index=False,
    #     header=False,
    # )
    # print(
    #     f"Chemical mentions generated at `{path_output_dir}/_chemical_mentions.tsv`. "
    #     "Use PubChem Identifier Exchange Service to get PubChem IDs and rename the "
    #     f"file to `{path_output_dir}/mention_to_cid.txt`. Press Enter to continue."
    # )
    # input()

    # # Get unique list of PubChem CIDs for the exchange service.
    # print("Generating PubChem CIDs...")
    # cids = pd.read_csv(
    #     f"{path_output_dir}/mention_to_cid.txt",
    #     sep='\t',
    #     header=None,
    # )[1].astype({1: 'Int64'}).dropna().unique()
    # cids = pd.DataFrame(cids)
    # cids.to_csv(
    #     f"{path_output_dir}/_cids.tsv",
    #     sep='\t',
    #     index=False,
    #     header=False,
    # )
    # print(
    #     f"PubChem CIDs generated at `{path_output_dir}/_cids.tsv`. "
    #     "Use PubChem Identifier Exchange Service to get FoodOn IDs and rename the "
    #     f"file to `{path_output_dir}/cid_to_chebi.txt`."
    # )


if __name__ == '__main__':
    main()

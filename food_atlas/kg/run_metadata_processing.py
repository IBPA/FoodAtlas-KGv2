import csv

import pandas as pd
from pandarallel import pandarallel
import click

from .preprocessing import standardize_chemical_conc

pandarallel.initialize(progress_bar=True)


CHAR_REPLACE = {
    'α': 'alpha',
    'β': 'beta',
    'ß': 'beta',
    'γ': 'gamma',
    'δ': 'delta',
    'ε': 'epsilon',
    'ζ': 'zeta',
    'η': 'eta',
    'θ': 'theta',
    'ι': 'iota',
    'κ': 'kappa',
    'λ': 'lambda',
    'μ': 'mu',
    'ν': 'nu',
    'ξ': 'xi',
    'ο': 'omicron',
    'π': 'pi',
    'ρ': 'rho',
    'σ': 'sigma',
    'τ': 'tau',
    'υ': 'upsilon',
    'φ': 'phi',
    'χ': 'chi',
    'ψ': 'psi',
    'ω': 'omega',
    '‐': '-',
    '−': '-',
    # '–': '-',
    '–': '-',
    '“': '"',
    '”': '"',
    '″': '"',
    '’': "'",
    '′': "'",
}


def standardize_food_part():
    pass


def standardize_chemical_name():
    pass


def replace_char(name):
    for old, new in CHAR_REPLACE.items():
        name = name.replace(old, new)

    return name


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
    row['_chemical_name'] = replace_char(row['_chemical_name'])
    row['source'] = 'lit2kg:gpt-4'
    row['reference'] = {
        'text': row['sentence'],
        'pmcid': row['pmcid'],
    }

    return row

@click.command()
@click.argument(
    'path-input',
)
@click.argument(
    'path-output-dir',
)
def main(
    path_input: str,
    path_output_dir: str,
):
    data_raw = pd.read_pickle(path_input)
    print(f"# sentences: {len(data_raw)}")

    data_raw['tuples'] = [[] for _ in data_raw.index]
    data_raw['tuples_not_parsed'] = [[] for _ in data_raw.index]
    data_raw.apply(parse_triplets_from_response, axis=1)
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
        'conc_value', 'conc_unit',
        'food_part', 'food_processing',
        'quality_score',
        'source', 'reference',
        '_food_name', '_chemical_name',
        '_conc', '_food_part',
    ]
    metadata = data_raw.explode('tuples').dropna(subset=['tuples'])
    metadata[columns] = None
    metadata = metadata.parallel_apply(format_tuple, axis=1)
    metadata = metadata[columns]

    # Stndardize the chemical concentration.
    metadata = standardize_chemical_conc(metadata)

    # TODO: Improve.
    # For now, just assign FoodOn IDs by exact matching for foods.
    lut_food = pd.read_csv(
        "outputs/kg/lookup_table_food.tsv",
        sep='\t',
    )
    lut_food = dict(zip(lut_food['name'], lut_food['foodatlas_id']))

    metadata = metadata[metadata['_food_name'].apply(lambda x: x in lut_food)]
    metadata.to_csv(
        f"{path_output_dir}/_metadata_new.tsv",
        sep='\t',
        index=False,
    )


if __name__ == '__main__':
    main()

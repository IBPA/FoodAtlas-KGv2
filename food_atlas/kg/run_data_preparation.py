import csv

import pandas as pd
import click

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
    '–': '-',
    '–': '-',
    '“': '"',
    '”': '"',
    '″': '"',
    '’': "'",
    '′': "'",
}


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
            row['triplets'] += [triplet]
        else:
            row['triplets_not_parsed'] += [triplet]


def format_triplet(
    row: pd.Series
):
    # errors = []
    row[['head', '_extracted_food_part', 'tail', '_extracted_conc']] \
        = [x.strip().lower() for x in row['triplets']]
    row['relationship'] = 'contains'
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
    data_lit2kg = pd.read_pickle(path_input)
    data_lit2kg['triplets'] = [[] for _ in data_lit2kg.index]
    data_lit2kg['triplets_not_parsed'] = [[] for _ in data_lit2kg.index]
    data_lit2kg.apply(parse_triplets_from_response, axis=1)
    data_lit2kg.to_csv(
        f"{path_output_dir}/_data_lit2kg_with_triplets_not_parsed.tsv",
        index=False,
    )

    n_triplets = 0
    n_triplets_not_parsed = 0
    for row in data_lit2kg.itertuples():
        n_triplets += len(row.triplets)
        n_triplets_not_parsed += len(row.triplets_not_parsed)
    print(
        f"{n_triplets_not_parsed} not parsed out of "
        f"{n_triplets + n_triplets_not_parsed} from {len(data_lit2kg)} sentences."
    )

    columns = [
        'head', 'relationship', 'tail', 'conc_value', 'conc_unit', 'food_part',
        'food_processing', 'source', 'reference', 'quality_score', '_extracted_conc',
        '_extracted_food_part',
    ]
    triplets = data_lit2kg.explode('triplets').dropna(subset=['triplets'])
    triplets[columns] = None
    triplets = triplets.apply(format_triplet, axis=1)
    triplets = triplets[columns]
    triplets['tail'] = triplets['tail'].apply(replace_char)
    # print(triplets)
    # exit()
    triplets.to_csv(
        f"{path_output_dir}/triplets_cleaned.tsv",
        sep='\t',
        index=False,
    )


if __name__ == '__main__':
    main()

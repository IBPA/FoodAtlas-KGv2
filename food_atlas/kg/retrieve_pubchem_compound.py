import os

import pandas as pd
import pubchempy as pcp
from tqdm import tqdm
import click

from .utils import load_lookup_tables, constants


def get_pubchem_compound(cid):
    c = pcp.Compound.from_cid(cid)
    return {
        'pubchem_cid': c.cid,
        'iupac_name': c.iupac_name,
        'synonyms': c.synonyms,
    }


@click.command()
@click.argument('path-output-dir', type=click.Path(exists=False))
def main(
    path_output_dir: str,
):
    if not os.path.exists(f"{path_output_dir}/_query_pies.txt"):
        print(
            "Please manually use the PubChem Identifier Exchange Service that can be "
            "found at https://pubchem.ncbi.nlm.nih.gov/idexchange/idexchange.cgi to "
            "retrieve PubChem CIDs."
        )
        print(
            "The input file containing the list of synonyms is at: "
            f"{path_output_dir}/_names_not_in_lut_chemical."
        )
        print(
            "Once finished, please rename and put at it as: "
            f"{path_output_dir}/_query_pies.txt."
        )
        exit()

    if os.path.exists(f"{path_output_dir}/_id_pubchem_compound.tsv"):
        print("PubChem Compound records already retrieved.")
        print("Press Ctrl+C to exit if you would like to not proceed.")

    data = pd.read_csv(
        f"{path_output_dir}/_query_pies.txt",
        delimiter='\t',
        header=None,
        names=['name', 'cid'],
        dtype={'cid': 'Int64'},
    )
    cids = data['cid'].dropna().unique().tolist()

    # Exclude those already in the knowledge graph.
    _, lut_chem = load_lookup_tables()
    cids = [
        x for x in cids
        if constants.get_lookup_key_by_id('pubchem_cid', x) not in lut_chem
    ]
    print(f"Retrieving {len(cids)} new NCBI Taxonomy IDs.")

    records = []
    for cid in tqdm(cids):
        records += [get_pubchem_compound(cid)]
    data = pd.DataFrame(records)

    data.to_csv(f"{path_output_dir}/_id_pubchem_compound.tsv", sep='\t')


if __name__ == '__main__':
    main()

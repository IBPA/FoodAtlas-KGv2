# -*- coding: utf-8 -*-
"""A run script for XXX.

More detailed description.

Attributes:
    attr1 (type): Description of attr1.

Authors:
    Fangzhou Li - fzli@ucdavis.edu

Todo:
    * TODOs

"""
import pandas as pd
import click

from . import KnowledgeGraph


@click.command()
@click.argument('path-output-dir', type=click.Path(exists=True))
def main(path_output_dir: str):
    kg = KnowledgeGraph(path_output_dir=path_output_dir)
    triplets = pd.read_csv(
        f"{path_output_dir}/triplets_cleaned.tsv",
        sep='\t',
    )
    triplets['_extracted_conc'] = triplets['_extracted_conc'].fillna('')
    triplets['_extracted_food_part'] = triplets['_extracted_food_part'].fillna('')
    kg.add_triplets(triplets)


if __name__ == '__main__':
    main()

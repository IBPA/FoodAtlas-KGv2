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
def main(
    path_output_dir: str
):
    kg = KnowledgeGraph()

    metadata = pd.read_csv(
        f"{path_output_dir}/_metadata_new.tsv",
        sep='\t',
    )
    metadata['_conc'] = metadata['_conc'].fillna('')
    metadata['_food_part'] = metadata['_food_part'].fillna('')

    kg.add_triplets_from_metadata(metadata)
    kg.save(path_output_dir)


if __name__ == '__main__':
    main()

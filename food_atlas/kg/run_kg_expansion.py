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
@click.argument(
    'path-input-metadata', type=click.Path(exists=True)
)
@click.option(
    '--path-input-kg', type=click.Path(exists=True), default="outputs/kg"
)
@click.option(
    '--path-output-dir', type=click.Path(exists=True), default="outputs/kg"
)
def main(
    path_input_metadata: str,
    path_input_kg: str,
    path_output_dir: str,
):
    kg = KnowledgeGraph(path_kg=path_input_kg)

    metadata = pd.read_csv(
        path_input_metadata,
        sep='\t',
    )
    metadata['_conc'] = metadata['_conc'].fillna('')
    metadata['_food_part'] = metadata['_food_part'].fillna('')

    kg.add_triplets_from_metadata(metadata)
    kg.save(path_output_dir)


if __name__ == '__main__':
    main()
